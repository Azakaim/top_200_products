from datetime import datetime, date, timedelta
from itertools import chain

import dateparser

from src.clients.ozon.schemas import ProductInfo, ArticlesResponseShema, Remainder, Datum


async def merge_stock_by_cluster(remains: list[dict]):
    clusters = {}
    for r in remains:
        for key, value in r.items():
            clusters[key] = str(int(clusters.get(key, 0)) + int(value))
    return clusters

async def collect_values_range_by_model(date_since: str,
                                        date_to: str,
                                        clusters_names: list,
                                        sheet_titles: list,
                                        model_name: str,
                                        model_posting: dict,
                                        remainders: list=None):
    values_range_by_model = []
    clusters_names = list(set(sheet_titles).intersection(set(clusters_names)))
    try:
        for ind, v in enumerate(model_posting):
            # работа с остатками FBO
            if model_name == "FBO":
                # получаем список диктов потому что могут быть одинаковые значения ключа
                remainders_count = [{r.cluster_name: str(r.available_stock_count)}
                                    for r in remainders if (str(r.sku) in list(v.keys())) and (r.cluster_name != "")]
                # склеиваем остатки по имени склада
                glued_remains = await merge_stock_by_cluster(remainders_count)
                # делаем заглушки для складов где товар не продается для корректного пуша в гугл таблицы
                prepared_remainders = await prepare_warehouse_stubs(glued_remains, clusters_names)
                print(prepared_remainders)
                sorted_remainders_by_column_name = await sort_remains_by_cluster_name(clusters_names, prepared_remainders)
                # расплющиваем в одномерный массив наш список
                values = ([model_name]
                          + list(v.keys())
                          + list(chain.from_iterable(v.values()))
                          + sorted_remainders_by_column_name)
            # работа с массивами FBS
            else:
                data_stub = ["" for _ in range(len(clusters_names))]
                # расплющиваем в одномерный массив наш список и добавляем заглушку
                # для ненужных данных по остаткам в кластерах
                values = [model_name] +  list(v.keys()) + list(chain.from_iterable(v.values())) + data_stub
            # добавляем остальные данные
            values.extend([date_since, date_to, datetime.now().strftime('%Y-%m-%dT%H:%M')])
            if values:
                values_range_by_model.append(values)
    except (ValueError, OverflowError, TypeError) as e:
        return e
    return values_range_by_model

async def prepare_warehouse_stubs(remainders: dict,clusters_info: list):
    clusters_count = len(clusters_info)
    missing_length = clusters_count - len(remainders)
    if missing_length > 0:
        # каким складам не хватает данных
        warehouse_count = list(remainders.keys()) # делаем это потому что объект -- dict_list а не list
        missing_warehouse = list(set(clusters_info) - set(warehouse_count))
        data_stub = { _: "" for _ in missing_warehouse}
        remainders.update(data_stub)
    return remainders

async def sort_remains_by_cluster_name(columns_names: list, remains: dict):
    sorted_postings = []
    key_remove = None
    if len(remains) != len(columns_names):
        #raise Exception(f"{remains}: магазины полный список --> {columns_names}")
        print(f"{remains}: магазины полный список --> {columns_names}")
    for cn in columns_names:
        for cluster_name in remains:
            if cn in cluster_name:
                key_remove = cluster_name
                sorted_postings.append(remains[cn])
                break
        # удаляем записанное значение
        remains.pop(key_remove)
    return sorted_postings

async def create_values_range(date_since: str,
                              date_to: str ,
                              clusters_names: list,
                              sheet_titles: list,
                              postings: dict,
                              remainders: list) -> list[list]:
    fbs_postings = next((val for key, val in postings.items() if "FBS" in key),None)
    fbo_postings = next((val for key, val in postings.items() if "FBO" in key),None)
    values_range = []
    fbo_res = []
    fbs_res = []
    if fbo_postings:
        fbo_res = await collect_values_range_by_model(date_since=date_since,
                                                      date_to=date_to,
                                                      clusters_names=clusters_names,
                                                      sheet_titles=sheet_titles,
                                                      model_name="FBO",
                                                      model_posting=fbo_postings,
                                                      remainders=remainders)

    if fbs_postings:
        fbs_res = await collect_values_range_by_model(date_since=date_since,
                                                      date_to=date_to,
                                                      clusters_names=clusters_names,
                                                      sheet_titles=sheet_titles,
                                                      model_name="FBS",
                                                      model_posting=fbs_postings)

    # добавляем созданные заголовки для таблицы и постинги
    values_range.extend([sheet_titles] + fbs_res + fbo_res)

    return values_range

async def parse_postings(postings_data: list[dict]) -> list:
    """
    Преобразует данные о доставке в нужный формат.

    :param postings_data: Список данных о доставке.
    :return: Список преобразованных данных.
    """
    parsed_postings = []

    for posting in postings_data:
        status = posting.get("status")
        products = posting.get("products", []) or []
        if products:
            # Преобразуем каждый продукт доставки в нужный формат
            chunks = [{str(prod.get("sku")): [prod.get("name"), prod.get("price"), status, str(prod.get("quantity"))]} for prod in products]
            parsed_postings.extend(chunks) # добавляем преобразованные продукты в общий список
    return parsed_postings

async def parse_skus(skus_data: list[dict]) -> list:
    parsed_skus = [ProductInfo(**s) for s in skus_data]
    skus = [s.sku for s in parsed_skus if s.sku != 0]
    return skus if skus else []

async def parse_articles(articles_data: dict) -> tuple:
    """
    :param articles_data: dict
    :return: tuple: articles, last_id, total
    """
    try:
        account_articles = ArticlesResponseShema(**articles_data)
    except (ValueError, OverflowError, TypeError) as e:
        raise Exception(e)
    return ([p.offer_id for p in account_articles.result.items],
            account_articles.result.last_id,
            account_articles.result.total)

async def parse_remainders(remainings_data: list) -> list:
    if remainings_data:
        return [Remainder(**r) for r in remainings_data]
    return []

async def collect_stats(acc_postings: tuple, acc_remainders: tuple, acc_analytics: tuple) -> tuple:
    acc_context, postings, remainders, analytics = None, None, None, None
    if acc_postings[0].account_id == acc_remainders[0].account_id == acc_analytics[0].account_id:
        acc_context = acc_remainders[0]
        postings = acc_postings[1]
        remainders = acc_remainders[1]
        analytics = acc_analytics[1]
    return acc_context, postings, remainders, analytics

async def get_converted_date(analytics_months: list):
    dates = {}
    for xdate in analytics_months:
        _month = xdate.split(" ")[0]
        parsed_date_first_date = dateparser.parse(xdate,
                                                  languages=["ru"],
                                                  settings={"PREFER_DAY_OF_MONTH": "first"})  # аналитика с первого
        parsed_date_last_date = dateparser.parse(xdate,
                                                 languages=["ru"],
                                                 settings={"PREFER_DAY_OF_MONTH": "last"})
        dates[_month] = [parsed_date_first_date, parsed_date_last_date]
    return dates

async def replace_warehouse_name_date(wname: str) -> str:
    return wname.replace("date", datetime.today().date().strftime("%d-%m"))

async def collect_titles(*, base_titles: list[str], clusters_names: list[str],months: list[str]) -> list[str]:
    count_col = len(clusters_names)
    base_titles[6] = await replace_warehouse_name_date(base_titles[6])
    base_titles[7] = await replace_warehouse_name_date(base_titles[7])
    rev_months_title = []
    orders_title = []
    # получаем точные даты месяца
    month_dates = await get_converted_date(months)
    for m in months:
        rev_months_title.extend(["Оборот " + m,
                           "Заказов " + m])
        orders_title.extend(["Заказы " + month_dates[m.split(' ')[0]][0].strftime("%d-%m"),
                             "Оборот " + month_dates[m.split(' ')[0]][0].strftime("%d-%m")])

    titles = base_titles[:8] + clusters_names + base_titles[8:9] + rev_months_title + base_titles[9:10] + orders_title + base_titles[10:]
    return titles

async def collect_clusters_names(remainders: list[Remainder]):
    # собираем все имена кластеров
    clusters_names = list(
        set([r.cluster_name for r in remainders
             if (r.cluster_name != "")]))
    return clusters_names

async def enrich_acc_context(base_sheets_titles: list,
                             remainders: list[Remainder], months: list[str]):
    """
    Updated cluster of names, title of sheet
    """
    clusters_names = await collect_clusters_names(remainders=remainders)
    sheet_titles = await collect_titles(base_titles=base_sheets_titles,
                                        clusters_names=clusters_names,
                                        months=months)
    return clusters_names, sheet_titles

async def remove_archived_skus(acc_remainders: list[tuple[object, list[Remainder], list[int]]] ,
                               all_analytics: list[tuple[object, tuple[str, list[Datum]]]]):
    # сортируем аналитику и возвраты по кабинетам tuple(контекст, tuple(возвраты, аналитика по месяцам))
    data = [(r[0], (r[2], a[1])) for r, a in zip(acc_remainders, all_analytics) if r[0].account_id == a[0].account_id]
    for d in data:
        # context
        acc_id = d[0].account_id
        # skus
        skus = d[1][0]
        # аналитика по месяцам
        analytics_by_months = d[1][1]
        for prod in analytics_by_months:
            # кладем список без архивных продуктов и кладем в аналитику соответствующего кабинета
            for x_analytics in all_analytics:
                if acc_id == x_analytics[0].account_id:
                    for ind, datum in enumerate(x_analytics[1]):
                        recollected_analytics = (datum[0],[a for a in prod[1] if int(a.dimensions[0].id) in skus and datum[0] == prod[0]])
                        if recollected_analytics[1]:
                            x_analytics[1][ind] = recollected_analytics
                            break

async def is_tuesday_today():
    today = date.today()
    if today.weekday() == 1: # от 0 - 6 где 1 - это вторник
        return True
    return False

async def get_week_range():
    today = date.today()
    monday = today - timedelta(days=1)
    week_ago = monday - timedelta(days=6)
    return week_ago, monday

async def check_orders_titles(table_date: list[list]):
    """
    The func checks the order headers if the last date in the month column is the last date or tuesday of the month,
    then it returns true or another false
    """
    titles = [f[0] for f in table_date]

    return titles