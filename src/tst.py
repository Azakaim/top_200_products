# sheet_titles = ["ID", "Name"]
# fbs_res = ["a", "b"]
# fbo_res = ["c"]
#
# values_range = []
# values_range.extend([sheet_titles, fbs_res, fbo_res])
# print(values_range)
# [['ID', 'Name'], ['a', 'b'], ['c']]
import calendar

import dateparser
from datetime import date

xdate = "Май 2025"
parsed_date_first_date = dateparser.parse(xdate, languages=["ru"], settings={"PREFER_DAY_OF_MONTH": "first"}) # аналитика с первого
parsed_date_last_date = dateparser.parse(xdate, languages=["ru"], settings={"PREFER_DAY_OF_MONTH": "last"}) # аналитика по последний день месяца
print(parsed_date_first_date.date(), parsed_date_last_date.date())
print()

# ,679158,2565634,2177098,736193,1236519,1779276,318487,668523,672855,787216,305686,492095,2313222,636782,2575011,2343769
# ,52060aa0-4e09-4d72-8aef-86b4cf0750ef,3362b8a4-788d-4bd3-a8a3-8e0fe02a1a45,2ba8c0e0-303d-40e6-b6b6-07aee69be2f1,fe624252-89dc-4fda-a1d3-7121c5433781,9096ce82-d175-4ba4-8568-801d8031934a,6bba22ac-43ec-43cd-a816-6e9b730891bb,4fd7926a-10d4-4e30-ad95-3d012b4eaf27,134efe4b-55bb-45fd-b0b4-b687d56c1f72,9e449fd5-1d24-45e3-a54b-ca3d43840741,72206048-1bbc-44f3-98b0-765f2816e680,edd4ba61-0636-45d9-9151-d457e5519eef,0c774bc3-d0b2-409b-a1c1-c7d800b07ae9,c8dc9ba6-ae18-49a2-846f-933858927976,72f36f91-8a3c-486e-8161-6eb4214e50eb,0779e6e6-dc08-4851-9dab-82a445733ff9,c312af2b-da35-452d-86a1-00b4cfe6135e