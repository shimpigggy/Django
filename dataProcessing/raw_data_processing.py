import pandas as pd
from openpyxl import load_workbook
import xlrd
import csv

root_path = '../data/'
count_observatory = 17


def oil_data_to_csv():
    raw_file_path = root_path + 'raw/oil.xlsx'
    new_file_path = root_path + '/raw/oil.csv'

    cell_start = 'A5'
    cell_end = 'D76'

    xlsx = load_workbook(raw_file_path, data_only=True)
    load_oil = xlsx['지역별소비(201812)']

    csv_new = open(new_file_path, 'w', encoding='utf-8', newline='')
    csv_writer = csv.writer(csv_new)
    csv_writer.writerow(['date', 'oil using'])

    # 지정된 값에서 데이터 가져오기
    get_cells = load_oil[cell_start: cell_end]
    year_base = '20'
    year = ''
    for row in get_cells:
        if not row[0].value is None:
            cell = row[0].value
            if '년' in cell:
                str = cell.split('년')
                year = year_base + str[0]
                month = str[1].split('월')
                month = int(month[0])
                oil_value = row[3].value
                csv_writer = oil_data_value_separate_per_day(year, month, oil_value, csv_writer)
                # csv_writer.writerow([year, month, row[3].value])

            else:
                month = cell.split('월')
                month = int(month[0])
                csv_writer = oil_data_value_separate_per_day(year, month, oil_value, csv_writer)
                # csv_writer.writerow([year, month, row[3].value])

    print("Raw Oil Data: xlsx -> csv           >>>>> Done")


def oil_data_value_separate_per_day(year, month, oil_value, csv_writer):
    if month in [1, 3, 5, 7, 8, 10, 12]:
        oil_value = oil_value / 31.0
        for day in range(1, 32):
            date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)
            csv_writer.writerow([date, oil_value])
        return csv_writer

    elif month == 2:
        if year == '2016':
            oil_value = oil_value / 29.0
            for day in range(1, 30):
                date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)
                csv_writer.writerow([date, oil_value])

        else:
            oil_value = oil_value / 28.0
            for day in range(1, 29):
                date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)
                csv_writer.writerow([date, oil_value])
        return csv_writer

    else:
        oil_value = oil_value / 30.0
        for day in range(1, 31):
            date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)
            csv_writer.writerow([date, oil_value])
        return csv_writer


def weather_data_to_csv():
    raw_file_path = root_path + 'raw/weather.csv'
    new_file_path = root_path + 'raw/processing_weather.csv'

    with open(raw_file_path, 'r') as raw_file:
        file = csv.reader(raw_file)

        csv_new = open(new_file_path, 'w', encoding='utf-8', newline='')
        csv_writer = csv.writer(csv_new)
        csv_writer.writerow(['date', 'temperate', 'wind_velocity', 'wind_direction', 'relative_humidity'])

        line_index = 0

        for row in file:
            if not line_index == 0:
                date = row[1]
                temperature = row[2]
                wind_velocity = row[3]
                wind_direction = row[4]
                relative_humidity = row[5]

                csv_writer.writerow([date, temperature, wind_velocity, wind_direction, relative_humidity])

            line_index += 1

    print("Raw weather Data: old csv -> csv     >>>>> Done")


def oil_data_open():
    oil_file_path = root_path + 'raw/oil.csv'
    pd_oil = pd.read_csv(oil_file_path)

    return pd_oil


def weather_data_open():
    weather_file_path = root_path + 'raw/processing_weather.csv'
    pd_weather = pd.read_csv(weather_file_path)

    return pd_weather


def combine_data_oil_weather():
    oil_data_to_csv()
    weather_data_to_csv()

    pd_oil = oil_data_open()
    pd_weather = weather_data_open()
    # print(pd_oil)
    # print(pd_weather)

    data = pd.merge(pd_weather, pd_oil)
    data.to_csv(root_path + 'combine_data.csv', index=False)

    print("Combine data OK")


def particulate_matter_data_csv():
    raw_file_path_root = root_path + 'raw/particulate/'
    separate = '/'
    file_extension = '.xls'

    new_file_path_root = root_path + 'raw/'
    new_file_extension = 'particulate_observatory.csv'

    for i in range(1, count_observatory):
        new_file_path = new_file_path_root + str(i) + new_file_extension

        csv_new = open(new_file_path, 'w', encoding='utf-8', newline='')
        csv_writer = csv.writer(csv_new)
        csv_writer.writerow(['date', 'PM-10', 'PM-2.5', 'O3', 'NO2', 'CO', 'SO2'])

        for j in range(2016, 2019):
            for k in range(1, 13):
                raw_file_path = raw_file_path_root + str(j) + separate + str(k) + '-' + str(i) + file_extension
                csv_writer = particulate_matter_file_open_and_write(raw_file_path, csv_writer)

    print("particulate matter Data: xlsx -> csv >>>> Done")


def particulate_matter_file_open_and_write(filename, csv_writer):
    xlsx = xlrd.open_workbook(filename)
    load_dust = xlsx.sheet_by_index(0)

    for row in range(load_dust.nrows):

        # print(str(load_dust.row_values(row)))
        if row > 1:
            date = load_dust.row_values(row)[0]
            PM10 = load_dust.row_values(row)[1]
            PM25 = load_dust.row_values(row)[2]
            ozon = load_dust.row_values(row)[3]
            C2N = load_dust.row_values(row)[4]
            CO = load_dust.row_values(row)[5]
            gas = load_dust.row_values(row)[6]

            csv_writer.writerow([date, PM10, PM25, ozon, C2N, CO, gas])

    return csv_writer


def raw_data_init():
    combine_data_oil_weather()
    particulate_matter_data_csv()


if __name__ == '__main__':
    raw_data_init()
