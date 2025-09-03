from datetime import datetime
from itertools import chain

from src.clients.ozon.schemas import ProductInfo, ArticlesResponseShema, Remainder
from src.pipeline.pipeline import PipelineSettings


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
    skus = [s.sku for s in parsed_skus]
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

async def collect_stats(acc_postings: tuple, acc_remainders: tuple) -> tuple:
    acc_context = acc_remainders[0]
    postings = acc_postings[1]
    remainders = acc_remainders[1]
    return acc_context, postings, remainders

async def collect_titles(*, base_titles: list[str], clusters_names: list[str]) -> list[str]:
    titles = base_titles[:6] + clusters_names + base_titles[6:]
    return titles

async def collect_clusters_names(remainders: list[Remainder]):
    # собираем все имена кластеров
    clusters_names = list(
        set([r.cluster_name for r in remainders
             if (r.cluster_name != "")]))
    return clusters_names

async def enrich_acc_context(context: PipelineSettings,
                             base_sheets_titles: list,
                             clusters_names: list,
                             remainders: list[Remainder]):
    """
    Updated cluster of names, title of sheet
    """
    context.clusters_names = await collect_clusters_names(remainders=remainders)
    context.sheet_titles = await collect_titles(base_titles=base_sheets_titles, clusters_names=clusters_names)
