import dataProcessing.raw_data_processing as raw
import pandas as pd

root_path = '../data/'
count_observatory = 17


def particulate_observatory_data_open(filename):
    pd_file = pd.read_csv(filename)
    pd_file = pd_file.dropna(thresh=2)
    return pd_file


def combine_data_oil_weather_open():
    file_path = root_path + 'combine_data.csv'
    pd_file = pd.read_csv(file_path)

    return pd_file


def combine_weather_oil_particulate():
    pd_combine_data = combine_data_oil_weather_open()

    file_path_root = root_path + '/raw/'
    file_path_tail = 'particulate_observatory.csv'

    pd_combine = []

    for i in range(1, count_observatory):
        file_path = file_path_root + str(i) + file_path_tail
        pd_particulate = particulate_observatory_data_open(file_path)
        # pd_particulate.to_csv('./data/asdf'+str(i)+'.csv', index=False)

        pd_combine.append(pd.merge(pd_particulate, pd_combine_data))

    return pd_combine


def combine_data_all():
    raw.raw_data_init()
    list_combine = combine_weather_oil_particulate()
    data = list_combine[0]

    for i in range(1, len(list_combine)):
        data = pd.concat([data, list_combine[i]])

    data = data.sort_values(['date'], ascending=[True])
    data.to_csv(root_path + 'data.csv', index=False)

    print("All Data combine >>>> OK :)")


if __name__ == '__main__':
    combine_data_all()
