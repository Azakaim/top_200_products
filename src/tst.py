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

