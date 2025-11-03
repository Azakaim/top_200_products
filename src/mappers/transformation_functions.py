import json
from collections import namedtuple, defaultdict
from datetime import datetime, date, timedelta
from itertools import chain
from typing import Type, Any, Literal
from zoneinfo import ZoneInfo

import dateparser
from transliterate import translit

from src.schemas.onec_schemas import OneCProductInfo, WareHouse, OneCProductsResults, OneCArticlesResponse, \
    OnecNomenclature, OneCNomenclatureCollection
from src.schemas.ozon_schemas import ProductInfo, Remainder
from src.dto.dto import Item, AccountStatsRemainders, AccountStatsAnalytics, AccountStats, \
    MonthlyStats, AccountStatsPostings, CollectionStats, PostingsProductsCollection, \
    PostingsDataByDeliveryModel, RemaindersByStock, AccountSortedCommonStats, SortedCommonStats, Period, Interval, \
    PostingsByPeriod, SkuInfo, ClusterInfo, TurnoverByPeriodSku, ProductsByArticle, AnalyticsSkuByMonths, \
    PostingsByPeriodQuantity


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

async def collect_stats(acc_postings: AccountStatsPostings,
                        acc_remainders: AccountStatsRemainders,
                        acc_analytics: AccountStatsAnalytics,
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
        # собираем все номенклатуры из 1С по соответствию ску кабинета
        for onecp in onec_nomenclature:
            for o in onecp.skus:
                if o.sku_fbo and o.sku_fbs:
                    if (o.sku_fbo and int(o.sku_fbo) in acc_remainders.skus) or \
                            (o.sku_fbs and int(o.sku_fbs) in acc_remainders.skus):
                        onec_products_info.append(onecp)

    return CollectionStats(ctx=acc_context,
                           postings=postings,
                           remainders=remainders,
                           monthly_analytics=monthly_analytics,
                           onec_nomenclatures=onec_products_info)

async def get_converted_date(unvalidated_dates: list):
    dates = {}
    if  any(x for x in unvalidated_dates if 'Z' in x):
        parsed_date_first_date = dateparser.parse(unvalidated_dates[0]).astimezone(ZoneInfo("Asia/Yekaterinburg"))
        parsed_date_last_date = dateparser.parse(unvalidated_dates[1]).astimezone(ZoneInfo("Asia/Yekaterinburg"))
        return {
            "first_day": parsed_date_first_date,
            "last_day": parsed_date_last_date
        }
    for xdate in unvalidated_dates:
        parts = xdate.split(" ")
        _month = parts[0] if parts else "some_date"
        # если указан месяц и год
        parsed_date_first_date = dateparser.parse(xdate,
                                                  settings={"PREFER_DAY_OF_MONTH": "first"})  # аналитика с первого
        parsed_date_last_date = dateparser.parse(xdate,
                                                 settings={"PREFER_DAY_OF_MONTH": "last"})

        dates[_month] = [parsed_date_first_date
                         .astimezone(ZoneInfo("Asia/Yekaterinburg")),
                         parsed_date_last_date.replace(hour=23,minute=59,second=59,microsecond=999999)
                         .astimezone(ZoneInfo("Asia/Yekaterinburg"))] # до конца дня
    return dates

async def replace_warehouse_name_date(wname: str) -> str:
    return wname.replace("date", datetime.today().date().strftime("%d-%m"))

async def collect_titles(*, base_titles: list[str],
                         clusters_names: list[str],
                         months: list[str] = None,
                         date_since: str = "",
                         date_to: str = "",
                         additions: list = None) -> list[str]:
    base_titles[6] = await replace_warehouse_name_date(base_titles[6])
    base_titles[7] = await replace_warehouse_name_date(base_titles[7])
    rev_months_title, orders_title, additional_tittles= [], [] , []
    # получаем точные даты месяца
    if months:
        rev_months_title, orders_title, additional_tittles = await generate_period_columns(months=months,
                                                                                           date_since=date_since,
                                                                                           date_to=date_to,
                                                                                           transliterations="ru",
                                                                                           additions=additions)

    titles = (base_titles[:8] + clusters_names + base_titles[8:9] +
              rev_months_title + base_titles[9:10] + orders_title +
              base_titles[10:12]) + additional_tittles + base_titles[12:]
    return titles


async def normalize_tittles_to_eng(list_to_normalize: list[str]) -> list[str]:
    normalized_tittles = []
    for x in list_to_normalize:
        words = []
        repl_w = x.split(" ")
        for w in repl_w:
            rw = translit(w, "ru", reversed=True)
            if "'" in rw:
                rw = rw.replace("'", "")
            elif "-" in rw:
                rw = rw.replace("-", "_")
            words.append(rw)
        normalized_tittles.append("_".join(words))
    return normalized_tittles


async def generate_period_columns(months: list[str],
                                  date_since: str,
                                  date_to: str,
                                  transliterations: Literal["ru", "en"],
                                  additions: list):
    month_titles, dynamic_titles, addition_titles = [], [], []
    turnover_word = "Оборот"
    orders_word = "Заказов"

    for m in months:
        # делаем это для формирования объекта для гугл щит
        if additions:
            addition_titles.extend([f"{a} {m}" for a in additions])
        month_titles.extend([f"{turnover_word} {m}",
                             f"{orders_word} {m}"])
    parsed_date_since = dateparser.parse(date_since).strftime("%d-%m")
    parsed_date_to = dateparser.parse(date_to).strftime("%d-%m")
    week_date = f"{parsed_date_since} {parsed_date_to}"
    dynamic_titles.extend([f"{turnover_word} {week_date}",
                          f"{orders_word} {week_date}"])
    if transliterations == "en":
        add_t = await normalize_tittles_to_eng(addition_titles)
        addition_titles.clear()
        addition_titles.extend(add_t)
        month_titles = await normalize_tittles_to_eng(month_titles)
        dynamic_titles = await normalize_tittles_to_eng(dynamic_titles)
    return month_titles, dynamic_titles, addition_titles

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

async def remove_archived_skus(acc_remainders: list[AccountStatsRemainders],
                               all_analytics: list[AccountStatsAnalytics]):
    # сортируем аналитику и возвраты по кабинетам tuple(контекст, tuple(возвраты, аналитика по месяцам))
    data = [AccountStats(ctx=r.ctx, skus=r.skus, monthly_analytics=a.monthly_analytics) for r, a in zip(acc_remainders, all_analytics) if r.ctx.account_id == a.ctx.account_id]
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
    return f"{week_ago}T00:00:00Z",f"{monday}T23:59:59.878Z"

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
        if status == 'cancelled':
            continue

        products = posting.get("products", []) or []
        if products:
            # добавляем преобразованные продукты в общий список
            posting_items.extend([
                Item(
                    sku_id=prod.get("sku"),
                    article=prod.get("offer_id"),
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

async def collect_common_stats(onec_products_info: list[OnecNomenclature],
                               stats_set: list[CollectionStats],
                               months_counter: int) -> SortedCommonStats:

    sorted_common_stats: list[AccountSortedCommonStats]  = []

    for s in stats_set:
        # для сортировки по складам
        remainders_by_warehouse = []
        remainders = []
        # по месяцам
        monthly_analytics = []

        postings = PostingsProductsCollection()
        postings.postings_fbs = PostingsDataByDeliveryModel(model="FBS")
        postings.postings_fbo = PostingsDataByDeliveryModel(model="FBO")

        # собираем всю аналитику в один список monthly_analytics
        await collect_common_analytics_by_month(monthly_analytics, s.monthly_analytics, months_counter)
        # собираем все остатки в один список
        remainders.extend(s.remainders)
        # собираем все доставки в один объект
        posting_items = [await sum_postings_by_sku(postings_items=
                                                   pc.postings_fbs.items +
                                                   pc.postings_fbo.items,
                                                   period=pc.period)
                         for pc in s.postings
        ]

        # сортируем remainders по складу
        await sort_common_remains_by_warehouse(remainders,remainders_by_warehouse)
        sorted_common_stats.append(AccountSortedCommonStats(
            remainders_by_stock=remainders_by_warehouse,
            monthly_analytics=monthly_analytics,
            postings_by_period=posting_items,
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
    cluster_names = {cn.cluster_id: cn.cluster_name for cn in remainings_data if cn.cluster_name != ''}
    await merge_unique_cluster_names(cluster_names, collected_remainders)

    for rem in collected_remainders:
        # Фильтруем по warehouse_id для точности (не зависит от форматирования имени)
        # Игнорим остатки с пустым cluster_name
        all_remainders_by_cluster = [r for r in remainings_data
                                     if r.cluster_id == rem.warehouse_id and r.cluster_name != '']
        if all_remainders_by_cluster:  # Проверяем что список не пустой (list comprehension никогда не возвращает None)
            rem.remainders.extend(all_remainders_by_cluster)

async def merge_unique_cluster_names(cluster_info: dict, collected_remainders: list[RemaindersByStock]):
    # имена складов для гугл таблицы айдишники для построения именованного тюпла
    # как ключ не принимает кирилические символы
    if len(collected_remainders) == 0:
        collected_remainders.extend([
            RemaindersByStock(warehouse_name=wname, warehouse_id=wid)
            for wid, wname in cluster_info.items()
        ])

async def sum_postings_by_sku(postings_items: list[Item], period: Period) -> PostingsByPeriod:
    items: dict[int, Item] = {}
    for pd in postings_items:
        if pd.sku_id in items:
            items[pd.sku_id].quantity += pd.quantity
        else:
            items[pd.sku_id] = pd
    return PostingsByPeriod(postings=list(items.values()), period=period)

async def get_cluster_info(sorted_stats: list[AccountSortedCommonStats], warehouse_id_to_name: dict):
    """
    :return :  cluster_ids , cluster_names
    """
    cluster_ids = set()
    cluster_names = {}
    for r in sorted_stats:
        # Фильтруем по warehouse_id (числа), а не по warehouse_name (строки)
        acc_cluster_names = {wh.warehouse_id: wh.warehouse_name
                            for wh in r.remainders_by_stock
                            if wh.warehouse_id not in cluster_ids}
        warehouse_id_to_name.update(acc_cluster_names)
        for k, n in acc_cluster_names.items():
            if k not in cluster_ids:
                cluster_ids.add(k)
                cluster_names[k] = n  # добавляем только если k не в cluster_ids
    return cluster_ids, [v for _, v in sorted(cluster_names.items())]

async def compare_cluster_to_remainder(names_title: list, wh_id, remainder_quantity: int):
    cl_be_rem = {}
    for field in list(names_title):
        if "id" in field:
            if str(wh_id) in field:
                cl_be_rem[field] = remainder_quantity
            else:
                cl_be_rem[field] = 0
    return cl_be_rem

async def calculate_sku_turnovers_and_postings(sku, postings_by_period: list[PostingsByPeriod]):
    """
    :return turnovers_by_periods, postings_by_period
    """
    # списки для сбора общего оборота по ску и общ-го кол-ва доставок
    price = 0
    turnovers_by_periods = []
    postings_quantity_by_period = []
    # перебираем периоды
    for pbp in postings_by_period:
        postings_quantity = await count_postings_quantity(sku, pbp.postings)
        # оборот за период
        turnover_by_period = await get_turnovers_by_periods(sku, postings_quantity, pbp)
        if price == 0:
            price = next((p.price for p in pbp.postings if sku == p.sku_id),0)
        turnovers_by_periods.append(turnover_by_period)
        # доставки за период
        postings_quantity_by_period.append(PostingsByPeriodQuantity(pbp.period, postings_quantity))
    return turnovers_by_periods, postings_quantity_by_period, price


async def sort_sku_by_price(flatten_postings: list[Item]):
    skus = {}
    for p in flatten_postings:
        if p.sku_id not in skus:
            skus[p.sku_id] = p.price
        else:
            if p.price < skus[p.sku_id]:
                skus[p.sku_id] = p.price
    return skus


async def collect_top_products_sheets_values_range(common_stats: SortedCommonStats,
                                                   base_top_sheet_titles: list[str],
                                                   months: list[str],
                                                   date_since:str,
                                                   date_to: str):

    # итоговый лист продуктов по артикулам
    products_by_article = []
    lk_names = []
    values_for_sheet_top_products = []
    warehouse_id_to_name = {}
    all_articles = {}

    # сортируем ску и цену для дальнейшего сопоставления
    flatten_postings = list(chain.from_iterable([ # раскатываю лист продуктов
        list(chain.from_iterable([p.postings for p in pbp.postings_by_period])) # раскатываю лист постингов
        for pbp in common_stats.sorted_stats
    ]))

    # прайс от магазина к магазину разный, поэтому здесь выставлен для каждого ску минимальный
    skus_by_price = await sort_sku_by_price(flatten_postings)

    # получаем id кластеров и имена, инициализируем дикт с ключами id кабинета: имя кластера
    cluster_ids, all_cluster_names = await get_cluster_info(common_stats.sorted_stats,
                                                            warehouse_id_to_name)
    # создаем заголовки для гугл таблицы
    title = await collect_titles(base_titles=base_top_sheet_titles,
                                 clusters_names=all_cluster_names,
                                 months=months,
                                 date_since=date_since,
                                 date_to=date_to,
                                 additions=["посетители","позиция в выдаче"])

    # добавляем первое значение для гугл таблицы - заголовки
    values_for_sheet_top_products.append(title)

    # Оптимизация: создаем словарь для быстрого поиска по артикулам из 1С
    onec_by_sku = {}
    for nom in common_stats.onec_nomenclatures:
        for sku_info in nom.skus:
            if sku_info.sku_fbo:
                onec_by_sku[int(sku_info.sku_fbo)] = nom
            if sku_info.sku_fbs:
                onec_by_sku[int(sku_info.sku_fbs)] = nom

    for cs in common_stats.sorted_stats:
        remainders_skus_info = await get_remainders_by_sku(all_cluster_names, cs.remainders_by_stock, skus_by_price)

        all_articles.update({art.article: art.prod_name for art in remainders_skus_info})
        # если в 1 с не будет строгого соответствия артикулу
        all_articles.update({'соответствие не найдено': None})
        lk_name = cs.account_name
        lk_names.append(lk_name)

        # Группируем SKU по артикулам для оптимизации
        skus_by_article = {}
        for remainder in remainders_skus_info:
            # Быстрый поиск в словаре вместо вызова aggregate_onec_info_by_article для каждого SKU
            onec_nom = onec_by_sku.get(remainder.sku)
            if onec_nom:
                onec_article = onec_nom.article
            else:
                onec_article = "соответствие не найдено"

            # Группируем по артикулу
            if onec_article not in skus_by_article:
                skus_by_article[onec_article] = []
            skus_by_article[onec_article].append(remainder)

        # Теперь обрабатываем сгруппированные данные - ОДИН ProductsByArticle на артикул
        for onec_article, sku_list in skus_by_article.items():
            # Создаем ОДИН ProductsByArticle для всего артикула, а не для каждого SKU
            # Собираем данные по каждому SKU артикула
            for remainder in sku_list:
                sku = remainder.sku

                # Получаем данные из 1С для этого SKU
                onec_nom = onec_by_sku.get(sku)
                if onec_nom:
                    chi6_remainders_quantity = await get_onec_remainders_quantity_by_cluster(onec_nom.stock, "Екатеринбург")
                    msk_remainders_quantity = await get_onec_remainders_quantity_by_cluster(onec_nom.stock, "Москва")
                    cost_price = onec_nom.cost_price_per_one
                else:
                    chi6_remainders_quantity = 0
                    msk_remainders_quantity = 0
                    cost_price = 0

                # Получаем аналитику по SKU
                analytics_by_sku_by_months = []
                try:
                    analytics_by_sku_by_months = await get_analytics_by_sku(sku, months, cs.monthly_analytics)
                except Exception as e:
                    print(f"Error getting analytics for SKU {sku}: {e}")

                # Получаем обороты и заказы по SKU
                (turnovers_by_periods,
                 postings_quantity_by_period,
                 price) = await calculate_sku_turnovers_and_postings(sku, cs.postings_by_period)

                # Считаем общие остатки по кластерам для этого SKU
                total_remainder_count = await sum_total_remainder_count_by_cluster(remainder)

                # Создаем ProductsByArticle для КАЖДОГО SKU (для правильной обработки в collect_sheets_values)
                # В products кладем только ЭТОТ SKU, а не все SKU артикула
                products_by_article.append(ProductsByArticle(
                    lk_name=lk_name,
                    article=onec_article,
                    remainders_chi6=chi6_remainders_quantity,
                    remainders_msk=msk_remainders_quantity,
                    cost_price=cost_price,
                    total_orders_by_period=postings_quantity_by_period,
                    total_remainder_count_by_clusters=total_remainder_count,
                    products=[remainder],  # ТОЛЬКО этот SKU, а не все SKU артикула
                    analytics_by_sku_by_months=analytics_by_sku_by_months,
                    turnovers_by_periods=turnovers_by_periods
                ))

    # собираем единый объект для добавления в табл
    row_number = 1
    for article in all_articles:
        lk_products_by_art = [art for art in products_by_article if article == art.article]
        if lk_products_by_art:
            row_number = await collect_sheets_values(
                lk_products_by_art,
                all_articles,
                values_for_sheet_top_products,
                row_number,
                cluster_ids
            )

    return values_for_sheet_top_products, len(cluster_ids)

async def collect_sheets_values(
    prod_by_art: list[ProductsByArticle],
    all_articles: dict,
    expanded_values: list[list[str]],
    row_number: int,
    cluster_ids: set
) -> int:
    """
    Формирует строки для Google таблицы: первая строка - артикул, под ней все SKU

    Args:
        prod_by_art: список продуктов по одному артикулу
        all_articles: словарь всех артикулов
        expanded_values: список для добавления строк
        row_number: текущий номер строки
        cluster_ids: ID кластеров для формирования колонок остатков

    Returns:
        int: обновленный номер строки
    """
    # если артикул == соответствие не найдено под ним все ску так же следует собрать
    if not prod_by_art:
        return row_number

    # Берем первый продукт для получения общей информации по артикулу
    first_product = prod_by_art[0]
    article = first_product.article
    prod_name = all_articles.get(article, "")

    # Суммарные данные по артикулу для первой строки
    total_chi6 = sum(p.remainders_chi6 for p in prod_by_art)
    total_msk = sum(p.remainders_msk for p in prod_by_art)
    total_cost_price = sum(p.cost_price for p in prod_by_art) / len(prod_by_art) if prod_by_art else 0

    # Остатки по кластерам (суммарные для артикула)
    cluster_remainders_total = {}
    for cluster_id in cluster_ids:
        cluster_remainders_total[cluster_id] = 0

    # Суммируем остатки по всем SKU артикула
    for product in prod_by_art:
        for sku_info in product.products:
            for cluster_info in sku_info.clusters_info:
                key = cluster_info.cluster_id
                if key in cluster_remainders_total:
                    # ИСПРАВЛЕНО: суммируем вместо перезаписи
                    cluster_remainders_total[key] += cluster_info.remainders_quantity or 0

    # Суммарный оборот и заказы - используем списки для сохранения порядка
    # Структура: [(period, turnover, orders), ...]
    periods_data = []
    period_map = {}  # {period_str: index}

    for product in prod_by_art:
        # Сначала обрабатываем обороты
        for turnover in product.turnovers_by_periods:
            period_key = turnover.period
            if period_key not in period_map:
                period_map[period_key] = len(periods_data)
                periods_data.append({
                    'period': turnover.period,
                    'turnover': turnover.turnover_by_period or 0,
                    'orders': 0
                })
            else:
                # ИСПРАВЛЕНО: суммируем обороты для всех SKU артикула
                periods_data[period_map[period_key]]['turnover'] += turnover.turnover_by_period or 0

        # Затем добавляем заказы
        for period_orders in product.total_orders_by_period:
            period = period_orders.period
            if period not in period_map:
                period_map[period] = len(periods_data)
                periods_data.append({
                    'period': period,
                    'turnover': 0,
                    'orders': 0
                })
            periods_data[period_map[period]]['orders'] += period_orders.quantity

    # Суммарная аналитика (посетители, позиция)
    total_analytics = {}
    for product in prod_by_art:
        for analytics in product.analytics_by_sku_by_months:
            month = analytics.month
            if month not in total_analytics:
                total_analytics[month] = {
                    "visitors": 0,
                    "orders_amount": 0,
                    "orders_quantity": 0,
                    "search_position": float('inf')  # Инициализируем бесконечностью для поиска минимума
                }
            total_analytics[month]["visitors"] += analytics.unique_visitors or 0
            total_analytics[month]["orders_amount"] += analytics.orders_amount or 0
            total_analytics[month]["orders_quantity"] += analytics.orders_quantity or 0
            # ИСПРАВЛЕНО: Берем лучшую (минимальную) позицию среди всех SKU артикула
            if analytics.search_position and analytics.search_position > 0:
                total_analytics[month]["search_position"] = min(
                    total_analytics[month]["search_position"],
                    analytics.search_position
                )

    # Первая строка - данные артикула (агрегированные)
    article_row = [
        str(row_number),  # № п/п
        article,  # Артикул ЛК (может быть пустым для первой строки)
        article,  # Артикул 1С
        "",  # SKU (пусто для строки артикула)
        prod_name,  # Наименование(
        "",  # колонка ЛК
        str(total_chi6),  # Ост. 1С ЧИ6
        str(total_msk),  # Ост. 1С МСК
    ]

    # Добавляем остатки по кластерам
    for cluster_id in sorted(cluster_ids):
        article_row.append(str(cluster_remainders_total.get(cluster_id, 0)))

    # Общий итог остатков по кластерам
    total_cluster_remainders = sum(cluster_remainders_total.values())
    article_row.append(str(total_cluster_remainders))

    # Разделяем периоды на месячные и недельные
    monthly_periods = [p for p in periods_data if hasattr(p['period'], 'period_type') and p['period'].period_type == Interval.MONTH]
    weekly_periods = [p for p in periods_data if hasattr(p['period'], 'period_type') and p['period'].period_type == Interval.WEEK]

    # оборот заказов за мес
    # ИСПРАВЛЕНО: используем заказы из постингов (без отменённых), а не из аналитики
    for m_per in monthly_periods:
        article_row.extend([str(m_per['turnover']), str(m_per['orders'])])

    # Динамика в обороте (можно добавить расчет)
    article_row.append("")  # TODO: расчет динамика

    # оборот заказов за неделю
    for w_per in weekly_periods:
        article_row.extend([str(w_per['turnover']), str(w_per['orders'])])

    # ФБС, ФБО (можно разделить если есть данные)
    article_row.extend(["", ""])

    for _, ta in sorted(total_analytics.items(),reverse=True):
        # посетители и позиция в выдаче по месяцам если их больше одного
        position = ta['search_position'] if ta['search_position'] != float('inf') else 0
        article_row.extend([ta['visitors'], round(position, 1)])

    # АЗП (средняя закупочная цена)
    article_row.append("")  # TODO: расчет АЗП

    # 1CЗП (закупочная цена из 1С)
    article_row.append(str(round(total_cost_price, 1)))

    # Комментарии
    article_row.append("")

    expanded_values.append([str(ar) for ar in article_row])
    row_number += 1

    # Строки для каждого SKU под артикулом
    for product in prod_by_art:
        for sku_info in product.products:

            sku_row = [
                "",  # № п/п (пусто для строк SKU)
                "",  # Артикул ЛК
                "",  # Артикул 1С
                str(sku_info.sku),  # SKU
                sku_info.prod_name,  # Наименование
                product.lk_name,  # ЛК
                "",  # Ост. 1С ЧИ6
                "",  # Ост. 1С МСК
            ]

            # ИСПРАВЛЕНО: Остатки по кластерам для ЭТОГО конкретного SKU
            sku_cluster_remainders = {}
            for cluster_id in sorted(cluster_ids):
                sku_cluster_remainders[cluster_id] = 0

            # Собираем остатки для текущего SKU
            for cluster_info in sku_info.clusters_info:
                if cluster_info.cluster_id in sku_cluster_remainders:
                    sku_cluster_remainders[cluster_info.cluster_id] = cluster_info.remainders_quantity or 0

            sku_row.extend([str(sku_cluster_remainders[cid]) for cid in sorted(cluster_ids)])

            # Общий итог остатков записывать для одного ску не нужно это нужно только для артикула
            sku_row.append("")

            # ИСПРАВЛЕНО: обрабатываем данные только ЭТОГО product (не перебираем все prod_by_art)
            turnovers_by_weeks = []
            monthly_turnovers = []

            # Создаем словарь аналитики по месяцам для быстрого поиска
            analytics_by_month = {
                analytics.month: analytics
                for analytics in product.analytics_by_sku_by_months
            }

            for turnover in product.turnovers_by_periods:
                # ИСПРАВЛЕНО: Для всех периодов (месячных и недельных) используем данные из постингов (total_orders_by_period)
                orders_qty = 0
                # Ищем количество заказов по периоду, сопоставляя по датам
                for order_period in product.total_orders_by_period:
                    # Сравниваем периоды по типу и датам
                    if (order_period.period.period_type == turnover.period.period_type and
                        order_period.period.start_date == turnover.period.start_date and
                        order_period.period.end_date == turnover.period.end_date):
                        orders_qty = order_period.quantity
                        break

                if turnover.period.period_type == Interval.MONTH:
                    monthly_turnovers.extend([str(turnover.turnover_by_period), str(orders_qty)])
                else:
                    turnovers_by_weeks.extend([str(turnover.turnover_by_period), str(orders_qty)])

            # Добавляем месячные обороты
            sku_row.extend(monthly_turnovers)

            # Динамика
            sku_row.append("")

            # добавляем недельные обороты
            sku_row.extend(turnovers_by_weeks)

            # ФБС, ФБО
            sku_row.extend(["", ""])

            # Аналитика по SKU (посетители, позиция)
            for analytics in product.analytics_by_sku_by_months:
                sku_row.extend([str(analytics.unique_visitors), str(round(analytics.search_position,1))])

            # АЗП, 1CЗП
            sku_row.append("")
            sku_row.append(str(round(product.cost_price, 2)))

            # цена товара
            sku_row.append(f"{sku_info.price}" if sku_info.price > 0 else "цена не определена/не было продаж")

            # Комментарии
            sku_row.append("")

            expanded_values.append(sku_row)

    return row_number

async def aggregate_onec_info_by_article(sku: int, onec_nomenclatures: list[OnecNomenclature]):
    """
    :return : onec_article, chi6_remainders_quantity, msk_remainders_quantity
    """
    onec_info_by_sku = await get_info_onec_by_sku(sku, onec_nomenclatures)
    if onec_info_by_sku is not None:
        # инфо из объекта onec
        onec_article = onec_info_by_sku.article
        chi6_remainders_quantity = await get_onec_remainders_quantity_by_cluster(onec_info_by_sku.stock,
                                                                                 "Екатеринбург")
        msk_remainders_quantity = await get_onec_remainders_quantity_by_cluster(onec_info_by_sku.stock,
                                                                                "Москва")
        cost_price = onec_info_by_sku.cost_price_per_one
    else:
        onec_article = "соответствие не найдено"
        chi6_remainders_quantity = 0
        msk_remainders_quantity = 0
        cost_price = 0
    return onec_article, chi6_remainders_quantity, msk_remainders_quantity, cost_price

async def get_turnovers_by_periods(sku: int, postings_quantity: int, postings_by_period: PostingsByPeriod):
    postings = postings_by_period.postings
    period_turnover = await calculate_turnover_by_sku(sku, postings_quantity, postings)
    period = postings_by_period.period
    # собираем оборот и доставки по определенному периоду
    period = Period(**period.__dict__)
    tbp = TurnoverByPeriodSku(period=period,
                              turnover_by_period=period_turnover if period_turnover else 0)
    return tbp

async def get_quantity_postings_by_period(sku: int, postings_by_period:  PostingsByPeriod):
    period = postings_by_period.period
    postings = postings_by_period.postings
    postings_quantity = await count_postings_quantity(sku, postings)
    # обращаем период в тюпл именованный
    PeriodInfo = namedtuple(
        "PeriodInfo", period.__dict__.keys())
    period = PeriodInfo(**period.__dict__)
    return period, postings_quantity

async def get_onec_remainders_quantity_by_cluster(stocks: list[WareHouse],
                                                  wh_name: str):
    return next((rq.quantity for rq in stocks if wh_name in rq.name),0)

async def upsert_sku_cluster(sku_info, r, q: int) -> None:
    # гарантируем уровень SKU
    clusters = sku_info.setdefault(r.sku, {
        "cluster_id": r.cluster_id,
        "article": r.offer_id,
        "prod_name": r.name,
    })

    # нормализуем имя кластера на всякий случай
    cluster_name = r.cluster_name.strip()

    # гарантируем уровень кластера внутри SKU
    entry = clusters.setdefault(cluster_name, {
        "quantity": 0,
    })

    # инкремент количества
    entry["quantity"] += q

async def enrich_sku_info_by_clusters(all_cluster_names: list, skus_info: dict[int, SkuInfo]):
    for k, v in skus_info.items():
        sku_clusters = [cm.cluster_name for cm in v.clusters_info]
        required_clusters = list(set(all_cluster_names) - set(sku_clusters))
        v.clusters_info.extend([ClusterInfo(cluster_name=x, remainders_quantity=0) for x in required_clusters])

async def get_remainders_by_sku(all_cluster_names: list[str],
                                remainder_by_stock: list[RemaindersByStock],
                                skus_by_price: dict) -> list[SkuInfo]:
    """
        Функция собирает объект SkuInfo который содержит параметры кластера и остатков в нем по sku
        Оптимизировано: использует defaultdict(list) для группировки кластеров
    """
    skus_info: dict[int, dict] = defaultdict(lambda: {"clusters": [], "article": None, "prod_name": None})

    # Собираем информацию о SKU и кластерах за один проход
    for rbs in remainder_by_stock:
        for r in rbs.remainders:
            # Вычисляем остатки один раз
            total_quantity = (r.available_stock_count + r.other_stock_count +
                             r.valid_stock_count + r.waiting_docs_stock_count)

            cluster = ClusterInfo(
                cluster_name=rbs.warehouse_name,
                cluster_id=rbs.warehouse_id,
                remainders_quantity=total_quantity
            )

            sku_data = skus_info[r.sku]
            if sku_data["article"] is None:
                sku_data["article"] = r.offer_id
                sku_data["prod_name"] = r.name
            sku_data["clusters"].append(cluster)

    # Преобразуем в SkuInfo объекты
    result = {
        sku: SkuInfo(
            sku=sku,
            article=data["article"],
            prod_name=data["prod_name"],
            clusters_info=data["clusters"],
            price=skus_by_price.get(sku,0)
        )
        for sku, data in skus_info.items()
    }

    # Добавляем недостающие кластеры
    await enrich_sku_info_by_clusters(all_cluster_names, result)
    return list(result.values())

async def get_info_onec_by_sku(sku: int, onec_nomenclatures: list[OnecNomenclature]):
    for si in onec_nomenclatures:
        if si.article != "":
            check_exist_skus = set()
            skus_by_trading_model = []
            for s in si.skus:
                if s.sku_fbo not in check_exist_skus and s.sku_fbo != "":
                    # добавляем в лист чекер ску
                    check_exist_skus.add(s.sku_fbo)
                    # добавляем в лист объекты sku если это тот sku который мы ищем
                    if str(sku) == s.sku_fbo:
                        skus_by_trading_model.append(s)
                if s.sku_fbs not in check_exist_skus and s.sku_fbs != "":
                    check_exist_skus.add(s.sku_fbs)
                    if str(sku) == s.sku_fbs:
                        skus_by_trading_model.append(s)
            # берем только не пустые sku так как по артикулам мы не сможем корректно сопоставить остатки
            if str(sku) in check_exist_skus:
                return OnecNomenclature(
                    article=si.article,
                    name=si.name,
                    stock=[WareHouse(name=w.name, quantity=w.quantity)
                        for w in si.stock],
                    skus=skus_by_trading_model,
                    cost_price_per_one=si.cost_price_per_one
                )
    return None

async def get_analytics_by_sku(sku: int, months: list, datums: list[MonthlyStats]) -> list[AnalyticsSkuByMonths]:
    """
    :param sku: int - SKU number
    :param months: list - Months number
    :param datums: list - Datums number
    :return: list - заказано на сумму , заказано товаров, уникальные посетители, позиция в выдаче

    Оптимизировано: использует словарь для быстрого поиска по месяцам и генератор вместо list comprehension
    """
    analytics_by_period = []
    sku_str = str(sku)

    # Создаем словарь для быстрого поиска по месяцам (O(1) вместо O(n))
    datums_by_month = {d.month: d for d in datums}

    for m in months:
        _month = m.split(' ')[0]
        d = datums_by_month.get(_month)

        if d and d.datum:
            # Используем генератор и next() вместо list comprehension для раннего выхода
            matching_item = next(
                (item for item in d.datum if str(item.dimensions[0].id) == sku_str),
                None
            )

            if matching_item:
                analytics_by_period.append(AnalyticsSkuByMonths(
                    month=m,
                    orders_amount=matching_item.metrics[0] if len(matching_item.metrics) > 0 else 0,
                    orders_quantity=matching_item.metrics[1] if len(matching_item.metrics) > 1 else 0,
                    unique_visitors=matching_item.metrics[2] if len(matching_item.metrics) > 2 else 0,
                    search_position=round(matching_item.metrics[3], 1) if len(matching_item.metrics) > 3 else 0,
                ))
                continue

        # Если данных нет - создаем нулевую запись
        analytics_by_period.append(AnalyticsSkuByMonths(
            month=m, orders_amount=0, orders_quantity=0, unique_visitors=0,search_position=0,
        ))

    return analytics_by_period

async def collect_onec_product_info(onec_products: OneCProductsResults, onec_articles: OneCArticlesResponse ):
    # Группировка по платформе и артикулу
    grouped_data = defaultdict(list)

    for skus_info in onec_products.onec_responses:
        key = None
        if len(skus_info.data.skus) == 0:
            key = skus_info.data.article if skus_info.data.article else f"None_{skus_info.data.uid}"
            grouped_data[key].append(skus_info.data)
        for s in skus_info.data.skus:
            # находим только для Озон связанные артикула ску
            if "ozon" in s.trading_platform.lower():
                if skus_info.data.article is not None:
                    key = skus_info.data.article
                else:
                    key = f"None_{skus_info.data.uid}"
                # убираем все ску связанные с другими мп
                new_onec_prod_info = await rebuild_onec_pro_info_by_trading_platform(skus_info.data)
                # добавляем ску по ключу
                grouped_data[key].append(new_onec_prod_info)
                break

    # пройдем по номенклатурам соберем ценник/кол-во = себестоимость
    art_cost_price = defaultdict()
    for nom in onec_articles.data:
        if not nom.article:
            key = nom.article if nom.article else f"None_{nom.uid}"
        else:
            key = nom.article
        art_cost_price[key] = nom.summ/nom.stock

    # объединяем словари в объекте номенклатуры собираем в список
    nomenclatures = []
    try:
        for art, cost_p in art_cost_price.items():
            for onec_prod_i in grouped_data[art]:
                onec_prod_i: OneCProductInfo
                nomenclatures.append(OnecNomenclature(
                    article=art,
                    name=onec_prod_i.name,
                    stock=onec_prod_i.stock,
                    skus=onec_prod_i.skus,
                    cost_price_per_one=cost_p
                    ))
    except Exception as e:
        print(e)
    return OneCNomenclatureCollection(onec_products=nomenclatures)

async def rebuild_onec_pro_info_by_trading_platform(onec_prod_info: OneCProductInfo ):
    skus_only_ozon = []
    for sku in onec_prod_info.skus:
        if "ozon"in sku.trading_platform.lower():
            skus_only_ozon.append(sku)
    onec_prod_info.skus = skus_only_ozon
    return onec_prod_info

async def sum_total_remainder_count_by_cluster(remainder: SkuInfo):
    return sum([q.remainders_quantity for q in  remainder.clusters_info])

async def count_postings_quantity(sku: int, postings: list[Item]):
    return sum([o.quantity for o in postings if o.sku_id == sku])

async def calculate_turnover_by_sku(sku: int, postings_quantity: int,  postings: list[Item]):
    price = next((o.price for o in postings if o.sku_id == sku), None) # берем прайс у любого из ску
    if price is None:
        return None
    turnover = price * postings_quantity
    return turnover if turnover is not None else 0

async def get_handling_period(months: list[str] = None) -> Period | list[Period]:
    periods = []
    month_data = await get_converted_date(months)
    if all(isinstance(v, datetime) for _, v  in month_data.items()):
        return Period(
            period_type=Interval.WEEK,
            start_date=month_data["first_day"],
            end_date=month_data["last_day"]
        )
    for mname, dates in month_data.items():
        period = Period(
            period_type=Interval.MONTH,
            month_name=mname,
            start_date=dates[0],
            end_date=dates[1]
        )
        periods.append(period)
    return periods

async def collect_account_auxiliary_table_values(
    base_titles: list[str],
    remainders: list[Remainder],
    postings: list[PostingsProductsCollection],
    clusters_names: list[str],
    date_since: str,
    date_to: str
) -> list[list]:
    """
    Собирает данные для вспомогательной таблицы по кабинету.

    Структура: Модель, SKU, Наименование, Цена, Статус, В заявке, [Кластеры], Дата от, Дата до, Дата обновления

    :param base_titles: базовые заголовки
    :param remainders: остатки по кабинету
    :param postings: доставки по кабинету (для получения цены, модели и количества в заявке)
    :param clusters_names: имена кластеров
    :param date_since: дата начала периода
    :param date_to: дата окончания периода
    :return: список строк для таблицы
    """
    # Формируем заголовки: базовые + динамические кластеры + даты
    headers = (base_titles[:6] + clusters_names +
               base_titles[6:9])

    values_range = [headers]

    # Собираем информацию из postings: цена, модель доставки, количество в заявке
    # ВАЖНО: берем только текущий период (неделю), не все периоды
    postings_info = {}  # {sku: {'price': float, 'model': str, 'in_delivery': int}}

    for posting_collection in postings:
        # Проверяем, что это текущий период (WEEK)
        if posting_collection.period and posting_collection.period.period_type == Interval.WEEK:
            # FBO доставки
            if posting_collection.postings_fbo and posting_collection.postings_fbo.items:
                for item in posting_collection.postings_fbo.items:
                    if item.sku_id not in postings_info:
                        postings_info[item.sku_id] = {
                            'price': item.price,
                            'model': 'FBO',
                            'in_delivery': 0
                        }
                    postings_info[item.sku_id]['in_delivery'] += item.quantity

            # FBS доставки
            if posting_collection.postings_fbs and posting_collection.postings_fbs.items:
                for item in posting_collection.postings_fbs.items:
                    if item.sku_id not in postings_info:
                        postings_info[item.sku_id] = {
                            'price': item.price,
                            'model': 'FBS',
                            'in_delivery': 0
                        }
                    postings_info[item.sku_id]['in_delivery'] += item.quantity

    # Группируем остатки по SKU
    skus_data = {}
    for r in remainders:
        if r.sku not in skus_data:
            # Получаем информацию из postings или используем дефолтные значения
            posting_data = postings_info.get(r.sku, {
                'price': 0.0,
                'model': 'FBO',  # дефолт
                'in_delivery': 0
            })

            skus_data[r.sku] = {
                'article': r.offer_id,
                'name': r.name,
                'price': posting_data['price'],
                'model': posting_data['model'],
                'in_delivery': posting_data['in_delivery'],
                'clusters': {}
            }

        # Собираем остатки по кластерам
        if r.cluster_name:
            total_quantity = (r.available_stock_count + r.other_stock_count +
                            r.valid_stock_count + r.waiting_docs_stock_count)
            if r.cluster_name in skus_data[r.sku]['clusters']:
                skus_data[r.sku]['clusters'][r.cluster_name] += total_quantity
            else:
                skus_data[r.sku]['clusters'][r.cluster_name] = total_quantity

    # Формируем строки данных
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M')

    for sku, data in skus_data.items():
        row = [
            data['model'],                      # Модель
            str(sku),                          # SKU
            data['name'],                      # Наименование
            str(data['price']),                # Цена
            'active',                          # Статус
            str(data['in_delivery']),          # В заявке
        ]

        # Добавляем остатки по кластерам в порядке заголовков
        for cluster_name in clusters_names:
            row.append(str(data['clusters'].get(cluster_name, 0)))

        # Добавляем даты
        row.extend([
            date_since,     # Дата от
            date_to,        # Дата до
            current_date    # Дата обновления
        ])

        values_range.append(row)

    return values_range
