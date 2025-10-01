import json
from datetime import datetime, date, timedelta
from itertools import chain
from typing import Type, Any

import dateparser

from src.schemas.onec_schemas import OneCProductInfo
from src.schemas.ozon_schemas import ProductInfo, Remainder
from src.dto.dto import Item, AccountMonthlyStatsRemainders, AccountMonthlyStatsAnalytics, AccountMonthlyStats, \
    MonthlyStats, AccountMonthlyStatsPostings, CollectionStats, PostingsProductsCollection, \
    PostingsDataByDeliveryModel, RemaindersByStock, AccountSortedCommonStats, SortedCommonStats


async def merge_stock_by_cluster(remains: list[dict]):
    clusters = {}
    for r in remains:
        for key, value in r.items():
            clusters[key] = str(int(clusters.get(key, 0)) + int(value))
    return clusters

async def collect_account_sheets_values_range_by_model(date_since: str,
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
        fbo_res = await collect_account_sheets_values_range_by_model(date_since=date_since,
                                                                     date_to=date_to,
                                                                     clusters_names=clusters_names,
                                                                     sheet_titles=sheet_titles,
                                                                     model_name="FBO",
                                                                     model_posting=fbo_postings,
                                                                     remainders=remainders)

    if fbs_postings:
        fbs_res = await collect_account_sheets_values_range_by_model(date_since=date_since,
                                                                     date_to=date_to,
                                                                     clusters_names=clusters_names,
                                                                     sheet_titles=sheet_titles,
                                                                     model_name="FBS",
                                                                     model_posting=fbs_postings)

    # добавляем созданные заголовки для таблицы и постинги
    values_range.extend([sheet_titles] + fbs_res + fbo_res)

    return values_range

async def collect_stats(acc_postings: AccountMonthlyStatsPostings,
                        acc_remainders: AccountMonthlyStatsRemainders,
                        acc_analytics: AccountMonthlyStatsAnalytics,
                        onec_nomenclature: list[OneCProductInfo]) -> CollectionStats:
    """
    Функция аккумулирует данные покабинетно в одном объекте
    """
    onec_products_info: list[OneCProductInfo] = []
    acc_context, postings, remainders, monthly_analytics = None, None, None, None
    if acc_postings.ctx.account_id == acc_remainders.ctx.account_id == acc_analytics.ctx.account_id:
        acc_context = acc_remainders.ctx
        postings = acc_postings.postings
        remainders = acc_remainders.remainders
        monthly_analytics = acc_analytics.monthly_analytics
        # собираем все номенклатуры из 1С по соответствиям в ску кабинета
        for onecp in onec_nomenclature:
            for o in onecp.skus:
                if o.sku_fbo and o.sku_fbs:
                    if (int(o.sku_fbo) or int(o.sku_fbs)) in acc_remainders.skus:
                        onec_products_info.append(onecp)

    return CollectionStats(ctx=acc_context,
                           postings=postings,
                           remainders=remainders,
                           monthly_analytics=monthly_analytics,
                           onec_nomenclatures=onec_products_info)

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
        dates[_month] = [parsed_date_first_date,
                         parsed_date_last_date.replace(hour=23,minute=59,second=59,microsecond=999999)] # до конца дня
    return dates

async def replace_warehouse_name_date(wname: str) -> str:
    return wname.replace("date", datetime.today().date().strftime("%d-%m"))

async def collect_titles(*, base_titles: list[str],
                         clusters_names: list[str],
                         months: list[str] = None,
                         date_since: str = "",
                         date_to: str = "") -> list[str]:
    # TODO нужно поменять параметр месяцы на недели. важно оставить недели текущего месяца и обновлять при наступлении нового
    base_titles[6] = await replace_warehouse_name_date(base_titles[6])
    base_titles[7] = await replace_warehouse_name_date(base_titles[7])
    rev_months_title = []
    orders_title = []
    # получаем точные даты месяца
    if months:
        for m in months :
            rev_months_title.extend(["Оборот " + m,
                                    "Заказов " + m])
            parsed_date_since = dateparser.parse(date_since).strftime("%d-%m")
            parsed_date_to = dateparser.parse(date_to).strftime("%d-%m")
            week_date = f"с {parsed_date_since} по {parsed_date_to}"
            orders_title.extend(["Заказы " + week_date,
                                 "Оборот " + week_date])

    titles = base_titles[:8] + clusters_names + base_titles[8:9] + rev_months_title + base_titles[9:10] + orders_title + base_titles[10:]
    print(titles)
    return titles

async def collect_clusters_names(remainders: list[Remainder]):
    # собираем все имена кластеров
    clusters_names = list(
        set([r.cluster_name for r in remainders
             if (r.cluster_name != "")]))
    return clusters_names

async def enrich_acc_context(base_sheets_titles: list,
                             remainders: list[Remainder]):
    """
    Updated cluster of names, title of sheet
    """
    clusters_names = await collect_clusters_names(remainders=remainders)
    sheet_titles = await collect_titles(base_titles=base_sheets_titles,
                                        clusters_names=clusters_names)
    return clusters_names, sheet_titles

async def remove_archived_skus(acc_remainders: list[AccountMonthlyStatsRemainders],
                               all_analytics: list[AccountMonthlyStatsAnalytics]):
    # сортируем аналитику и возвраты по кабинетам tuple(контекст, tuple(возвраты, аналитика по месяцам))
    data = [AccountMonthlyStats(ctx=r.ctx, skus=r.skus, monthly_analytics=a.monthly_analytics) for r, a in zip(acc_remainders, all_analytics) if r.ctx.account_id == a.ctx.account_id]
    for d in data:
        # аналитика по месяцам
        for prod in d.monthly_analytics:
            # кладем список без архивных продуктов в аналитику соответствующего кабинета
            for x_analytics in all_analytics:
                if d.ctx.account_id == x_analytics.ctx.account_id:
                    for ind, data in enumerate(x_analytics.monthly_analytics):
                        new_datum = [x for x in prod.datum if int(x.dimensions[0].id) in d.skus and data.month == prod.month]
                        recollected_analytics =  MonthlyStats(month=data.month,datum=new_datum if new_datum is not None else [])
                        if recollected_analytics.datum:
                            x_analytics.monthly_analytics[ind] = recollected_analytics
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
    return f"{week_ago}T00:00:00Z",f"{monday}T23:59:59Z"

async def check_orders_titles(table_date: list[list]):
    """
    The func checks the order headers if the last date in the month column is the last date or tuesday of the month,
    then it returns true or another false
    """
    titles = [f[0] for f in table_date]

    return titles

async def parse_obj_by_type_base_cls(obj: str | dict | None, obj_type: Type[Any]):
    if isinstance(obj, dict):
        return obj_type(**obj)
    if isinstance(obj, str):
        d = json.loads(obj)
        if isinstance(d, list):
            if all(isinstance(o, dict) for o in d):
                return [obj_type(**o) for o in d]
        elif isinstance(d, dict):
            return obj_type(**d)
    return None

async def parse_postings(postings_data: list[dict]) -> list:
    """
    Преобразует данные о доставке в нужный формат.

    :param postings_data: Список данных о доставке.
    :return: Список преобразованных данных.
    """
    posting_items = []
    for posting in postings_data:
        status = posting.get("status")
        if status != 'cancelled':
            products = posting.get("products", []) or []
        else:
            continue
        if products:
            # добавляем преобразованные продукты в общий список
            posting_items.extend([
                Item(
                    sku_id=prod.get("sku"),
                    title=prod.get("name"),
                    price=prod.get("price"),
                    status=status,
                    quantity=prod.get("quantity")
                )
                for prod in products if prod.get("sku")
            ])
    return posting_items

async def parse_skus(skus_data: list[dict]) -> list:
    parsed_skus = [ProductInfo(**s) for s in skus_data]
    skus = [s.sku for s in parsed_skus if s.sku != 0]
    return skus if skus else []

async def parse_remainders(remainings_data: list) -> list:
    if remainings_data:
        return [Remainder(**r) for r in remainings_data]
    return []

async def collect_common_stats(onec_products_info: list[OneCProductInfo],
                               stats_set: list[CollectionStats],
                               months_counter: int) -> SortedCommonStats:
    remainders = []
    sorted_common_stats: list[AccountSortedCommonStats]  = []
    monthly_analytics = []
    for s in stats_set:
        postings = PostingsProductsCollection()
        postings.postings_fbs = PostingsDataByDeliveryModel(model="FBS")
        postings.postings_fbo = PostingsDataByDeliveryModel(model="FBO")
        # дял сортировки по складам
        remainders_by_warehouse = []
        # собираем всю аналитику в один список monthly_analytics
        await collect_common_analytics_by_month(monthly_analytics, s.monthly_analytics, months_counter)
        # собираем все остатки в один список
        remainders.extend(s.remainders)
        # собираем все доставки в один объект
        posting_items = await sum_postings_by_sku(s.postings.postings_fbs.items +
                                                  s.postings.postings_fbo.items)

        # сортируем remainders по складу
        await sort_common_remains_by_warehouse(remainders,remainders_by_warehouse)
        sorted_common_stats.append(AccountSortedCommonStats(
            remainders_by_stock=remainders_by_warehouse,
            monthly_analytics=monthly_analytics,
            posting_items=posting_items,
            account_id=s.ctx.account_id,
            account_name=s.ctx.account_name
        ))

    return SortedCommonStats(
        onec_nomenclatures=onec_products_info,
        sorted_stats=sorted_common_stats
    )

async def collect_common_analytics_by_month(common_monthly_analytics: list[MonthlyStats],
                                            acc_monthly_analytics:list[MonthlyStats],
                                            months_counter: int):
    for m in acc_monthly_analytics:
        existing: MonthlyStats = next((d for d in common_monthly_analytics if d.month == m.month),None)
        if existing is not None:
            existing.datum.extend(m.datum)
        elif len(common_monthly_analytics) < months_counter:
            monthly_analytics = MonthlyStats(month=m.month,datum=m.datum)
            common_monthly_analytics.append(monthly_analytics)

async def sort_common_remains_by_warehouse(remainings_data: list[Remainder], collected_remainders: list[RemaindersByStock]) :
    cluster_names = list(set([cn.cluster_name for cn in remainings_data]))
    await merge_unique_warehouse(cluster_names, collected_remainders)

    for i, rem in enumerate(remainings_data):
        all_remainders_by_sku = [r for r in remainings_data if r.cluster_name == rem.cluster_name]
        required_stock = next((cr for cr in collected_remainders if cr.warehouse_name == rem.cluster_name), None)
        required_stock.remainders.extend(all_remainders_by_sku)

async def merge_unique_warehouse(cluster_names: list[str], collected_remainders: list[RemaindersByStock]):
    if len(collected_remainders) == 0:
        collected_remainders.extend([RemaindersByStock(warehouse_name=name) for name in cluster_names])
    if not all(x for x in collected_remainders if x.warehouse_name in cluster_names):
        required_n = [x.warehouse_name for x in collected_remainders if x.warehouse_name not in cluster_names]
        for n in required_n:
            collected_remainders.append(RemaindersByStock(warehouse_name=n))

async def sum_postings_by_sku(postings_items: list[Item]) -> list[Item]:
    items: list[Item] = []
    for pd in postings_items:
        if len(items) != 0:
            el: Item = next((s for s in items if s.sku_id == pd.sku_id), None)
            if el is not None:
                el.quantity += pd.quantity
            else:
                items.append(pd)
        else:
            items.append(pd)
    return items

async def collect_top_products_sheets_values_range(common_stats: SortedCommonStats,
                                                   base_top_sheet_titles: list[str],
                                                   months: list[str],
                                                   date_since:str,
                                                   date_to: str):
    values_for_sheet_top_products = []
    cluster_names = []
    for r in common_stats.sorted_stats:
        acc_clusters_name = [wh_n.warehouse_name for wh_n in r.remainders_by_stock if wh_n.warehouse_name != '']
        cluster_names.extend(acc_clusters_name)
    uniq_cluster_names = list(set(cluster_names))
    title = await collect_titles(base_titles=base_top_sheet_titles,
                                 clusters_names=uniq_cluster_names,
                                 months=months,
                                 date_since=date_since,
                                 date_to=date_to)
    values_for_sheet_top_products.append(title)
    for cs in common_stats.sorted_stats:
        for nom in common_stats.onec_nomenclatures:
            ...
