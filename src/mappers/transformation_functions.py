import json
from collections import namedtuple
from datetime import datetime, date, timedelta
from itertools import chain
from typing import Type, Any, Literal
from zoneinfo import ZoneInfo

import dateparser
from transliterate import translit

from src.schemas.onec_schemas import OneCProductInfo, WareHouse, OneCProductsResults, OneCArticlesResponse, \
    OnecNomenclature, OneCNomenclatureCollection
from src.schemas.ozon_schemas import ProductInfo, Remainder, Datum
from src.dto.dto import Item, AccountStatsRemainders, AccountStatsAnalytics, AccountStats, \
    MonthlyStats, AccountStatsPostings, CollectionStats, PostingsProductsCollection, \
    PostingsDataByDeliveryModel, RemaindersByStock, AccountSortedCommonStats, SortedCommonStats, Period, Interval, \
    PostingsByPeriod, SkuInfo, ClusterInfo, TurnoverByPeriodSku, ProductsByArticle, AnalyticsSkuByMonths


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
                    if (int(o.sku_fbo) or int(o.sku_fbs)) in acc_remainders.skus:
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
                                                  languages=["ru"],
                                                  settings={"PREFER_DAY_OF_MONTH": "first"})  # аналитика с первого
        parsed_date_last_date = dateparser.parse(xdate,
                                                 languages=["ru"],
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
        if status != 'cancelled':
            products = posting.get("products", []) or []
        else:
            continue
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
    monthly_analytics = []

    for s in stats_set:
        postings = PostingsProductsCollection()
        postings.postings_fbs = PostingsDataByDeliveryModel(model="FBS")
        postings.postings_fbo = PostingsDataByDeliveryModel(model="FBO")
        # для сортировки по складам
        remainders_by_warehouse = []
        remainders = []
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
        # игнорим все что связано с пустым кластером
        all_remainders_by_cluster = [r for r in remainings_data if r.cluster_name == rem.warehouse_name]
        if all_remainders_by_cluster is None:
            continue
        rem.remainders.extend([r for r in all_remainders_by_cluster]) # передаем по required_stock этой ссылке

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
    cluster_names = []
    for r in sorted_stats:
        acc_cluster_names = {wh.warehouse_id: wh.warehouse_name for wh in r.remainders_by_stock if wh.warehouse_name not in cluster_ids}
        warehouse_id_to_name.update(acc_cluster_names)
        for k, n in acc_cluster_names.items():
            if k not in cluster_ids:
                cluster_ids.add(k)
            if n not in cluster_names:
                cluster_names.append(n)
    return cluster_ids, cluster_names

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
    turnovers_by_periods = []
    postings_quantity_by_period = []
    # перебираем периоды
    for pbp in postings_by_period:
        postings_quantity = await count_postings_quantity(sku, pbp.postings)
        # оборот за период
        turnover_by_period = await get_turnovers_by_periods(sku, postings_quantity, pbp)
        turnovers_by_periods.append(turnover_by_period)
        # доставки за период
        postings_quantity_by_period.append((pbp.period, postings_quantity))
    return turnovers_by_periods, postings_quantity_by_period

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
    all_articles = set()

    # получаем id кластеров и имена, инициализируем дикт с ключами id кабинета: имя кластера
    cluster_ids, all_cluster_names = await get_cluster_info(common_stats.sorted_stats,
                                                            warehouse_id_to_name)
    # создаем заголовки дял гугл таблицы
    title = await collect_titles(base_titles=base_top_sheet_titles,
                                 clusters_names=all_cluster_names,
                                 months=months,
                                 date_since=date_since,
                                 date_to=date_to,
                                 additions=["посетители","позиция в выдаче"])

    # добавляем первое значение для гугл таблицы - заголовки
    values_for_sheet_top_products.append(title)
    # формируем заголовки для именованного тюпла
    turnover_columns, orders_columns, addition_tittles = await generate_period_columns(months,
                                                                                       date_since,
                                                                                       date_to,
                                                                                       "en",
                                                                                       ["посетители","позиция в выдаче"])
    # объявляем именованный тюпл для удобства сбора по ску remainders
    fields = ([ "n", "lk_article", "onec_article",
                "sku", "product_name",
                "lk_name", "onec_remainders_chi6",
                 "onec_remainders_msk"] + [f"id_{i}" for i in cluster_ids] +
                ["common_result"] + turnover_columns +
                ["dynamics_turnover_by_months"] +
                orders_columns + ["azp", "onec_zp"] +
                addition_tittles)
    # объявляем именованный тюпл для сбора объектов для добавления в таблицу
    SheetValue = namedtuple("SheetValue", fields)
    for cs in common_stats.sorted_stats:
        remainders_skus_info = await get_remainders_by_sku(all_cluster_names, cs.remainders_by_stock)
        all_articles = {art.article for art in remainders_skus_info}
        lk_name = cs.account_name
        lk_names.append(lk_name)
        for remainder in remainders_skus_info:
            total_remainder_count = await sum_total_remainder_count_by_cluster(remainder)
            sku = remainder.sku
            # берем аналитику по ску
            analytics_by_sku_by_months = await get_analytics_by_sku(sku, months, cs.monthly_analytics)

            # списки для сбора общего оборота по ску и общ-го кол-ва доставок
            (turnovers_by_periods ,
             postings_quantity_by_period) = await calculate_sku_turnovers_and_postings(sku,cs.postings_by_period)

            # берем артикул в 1с остатки
            (onec_article,
             chi6_remainders_quantity,
             msk_remainders_quantity,
             cost_price) = await aggregate_onec_info_by_article(sku, common_stats.onec_nomenclatures)

            products_sorted_by_art = [r for r in remainders_skus_info
                                      if r.article == onec_article
                                      and sku == r.sku]
            products_by_article.append(ProductsByArticle(
                lk_name=lk_name,
                article=onec_article,
                remainders_chi6=chi6_remainders_quantity,
                remainders_msk=msk_remainders_quantity,
                cost_price=cost_price,
                total_orders_by_period=postings_quantity_by_period,
                total_remainder_count_by_clusters=total_remainder_count,
                products=products_sorted_by_art,
                analytics_by_sku_by_months=analytics_by_sku_by_months,
                turnovers_by_periods=turnovers_by_periods
            ))
    for article in all_articles:
        lk_products_by_art = [art for art in products_by_article if article == art.article]
        for lk_p in lk_products_by_art:
            print(lk_p)


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
    # обращаем период в тюпл именованный
    PeriodInfo = namedtuple(
        "PeriodInfo", period.__dict__.keys())
    period = PeriodInfo(**period.__dict__)
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

async def unpack_sku_info(skus_info: dict):
    skus_by_cluster = []
    META_KEYS = {"article", "cluster_id", "prod_name"}

    for k, v in skus_info.items():
        sku = k
        cluster_id = v.get("cluster_id")
        article = v.get("article")
        prod_name = v.get("prod_name")
        if isinstance(v, dict):
            # для сбора инфы по кластерам
            clusters_info = []
            for cluster_name, q in v.items():
                if cluster_name in META_KEYS:
                    continue
                clusters_info.append(
                    ClusterInfo(
                        cluster_name=cluster_name,
                        cluster_id=cluster_id,
                        remainders_quantity=q["quantity"],
                    )
                )
            # собираем список ску инфо с кластерами
            skus_by_cluster.append(
                SkuInfo(
                sku=sku,
                article=article,
                prod_name=prod_name,
                clusters_info=clusters_info
            ))
    return skus_by_cluster

async def enrich_sku_info_by_clusters(all_cluster_names: list, sku_info: dict):
    for k, v in sku_info.items():
        for cluster_name in all_cluster_names:
            if cluster_name not in v:
                v[cluster_name] = {"quantity": 0}

async def get_remainders_by_sku(all_cluster_names: list[str], remainder_by_stock: list[RemaindersByStock]) -> list[SkuInfo]:
    skus_info: list[SkuInfo] = []
    for rbs in remainder_by_stock:
        clusters = []
        ClusterInfo(
            cluster_name=rbs.warehouse_name,
            cluster_id=rbs.warehouse_id
        )
        for r in rbs.remainders:
            # считаем остатки
            q = (r.available_stock_count + r.other_stock_count +
                 r.valid_stock_count + r.waiting_docs_stock_count)
    #TODO доделать так что бы возращасля список ску а не 1 
    #         skus_info.append(SkuInfo(
    #             sku=r.sku,
    #             article=r.article,
    #             prod_name=r.name,
    #             clusters_info=)
    # await enrich_sku_info_by_clusters(all_cluster_names, sku_info)
    # skus_by_cluster = await unpack_sku_info(sku_info)
    # return skus_by_cluster

async def get_info_onec_by_sku(sku: int, onec_nomenclatures: list[OnecNomenclature]):
    for si in onec_nomenclatures:
        if si.article != "":
            check_exist_skus = set()
            skus_by_trading_model = []
            for s in si.skus:
                if s.trading_platform.lower() == "ozon":
                        if s.sku_fbo not in check_exist_skus and s.sku_fbo != "":
                            # добавляем в лист чекер ску
                            check_exist_skus.add(s.sku_fbo)
                            # добавляем в лист объекты ску если это тот ску который мы ищем
                            if str(sku) == s.sku_fbo:
                                skus_by_trading_model.append(s)
                        if s.sku_fbs not in check_exist_skus and s.sku_fbs != "":
                            check_exist_skus.add(s.sku_fbs)
                            if str(sku) == s.sku_fbs:
                                skus_by_trading_model.append(s)
            # берем только не пустые ску так как по артикулам мы не сможем корректно сопоставить остатки
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
    :return: list - заказано на сумму , заказано товаров, уникальные посетители
    """
    analytics_by_period = []
    for m in months:
        for d in datums:
            if d.month not in m:
                continue
            res = [AnalyticsSkuByMonths(month=m,
                                        orders_amount=s.metrics[0],
                                        orders_quantity=s.metrics[1],
                                        unique_visitors=s.metrics[2])
                   for s in d.datum if str(s.dimensions[0].id) == str(sku)]
            if res:
                analytics_by_period.extend(res)
    return analytics_by_period

async def collect_onec_product_info(onec_products: OneCProductsResults, onec_articles: OneCArticlesResponse ):
    onec_prods = []
    for nom in onec_articles.data:
        o_pr = [op.data for op in onec_products.onec_responses if nom.article == op.data.article]
        price_per_one_prod = next((n.summ/n.stock for n in onec_articles.data if n.stock and nom.article == n.article),None)
        for prod in o_pr:
            onec_prods.append(OnecNomenclature(
                article=prod.article,
                name=prod.name,
                stock=prod.stock,
                skus=prod.skus,
                cost_price_per_one=price_per_one_prod
            ))
    return OneCNomenclatureCollection(onec_products=onec_prods)

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
