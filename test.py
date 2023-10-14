import jdatetime

def is_valid_day_in_jalali_month(jalali_year, jalali_month, day):

    try:
        return jdatetime.date(jalali_year, jalali_month, day)

    except ValueError:
        nearest_valid_date = None
        current_day = day
        while nearest_valid_date is None:
            current_day -= 1
            try:
                nearest_valid_date = jdatetime.date(jalali_year, jalali_month, current_day)
            except ValueError:
                continue
        return nearest_valid_date

jalali_year_to_check = 1402
jalali_month_to_check = 10
jalali_day_to_check = 31

print( is_valid_day_in_jalali_month(jalali_year_to_check, jalali_month_to_check, jalali_day_to_check) )

