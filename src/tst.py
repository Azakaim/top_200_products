# sheet_titles = ["ID,Name"]
# fbs_res = ["a,b"]
# fbo_res = ["c"]
#
# values_range = []
# values_range.extend([sheet_titles fbs_res fbo_res])
# print(values_range)
# [['ID' 'Name'] ['a' 'b'] ['c']]
# import calendar
#
from collections import namedtuple

import dateparser
from transliterate import translit

# from datetime import date timedelta
#
# # xdate = "Май 2025"
# # parsed_date_first_date = dateparser.parse(xdate languages=["ru"] settings={"PREFER_DAY_OF_MONTH": "first"}) # аналитика с первого
# # parsed_date_last_date = dateparser.parse(xdate languages=["ru"] settings={"PREFER_DAY_OF_MONTH": "last"}) # аналитика по последний день месяца
# # print(parsed_date_first_date.date() parsed_date_last_date.date())
# # print()
#
# today = date.today()
# print(today.weekday())
#
#
# monday = today - timedelta(days=1)
# week_ago = monday - timedelta(days=6)
# print(week_ago monday)
# 
# import base64
# creds = "user:password".encode()
# token =  base64.b64encode(creds).decode()
# print(token)

l = [translit("Москва", reversed=True).lower(), translit("Питер", reversed=True).lower()]
print(l)
offer = namedtuple(
    "Offer",["offer_id", "sku", "name_product",] + l)
o1 = offer(sku=1,offer_id=2,name_product="12",moskva=1, piter=0)
o2 = offer(sku=2,offer_id=3,name_product="23",moskva=10,piter=2)

print(list(o1))
print(list(o2))
