import argparse
import calendar
import datetime
import io
import locale
import os
import sys
import warnings
from functools import reduce
from pathlib import Path, PurePath
from typing import Union

import pandas as pd
import xlwings as xw
from loguru import logger
from xlrd import XLRDError

from Colors import Colors
from FormattedWorkbook import FormattedWorkbook
from MyLoggingException import MyLoggingException

PROGRAM_NAME = Path(__file__).stem
PROGRAM_VERSION = "0.6.2"


class WeeklyReport:
    def __init__(self):
        self.log_level = 'ERROR'

        today_datetime = datetime.datetime.now()

        logger.remove()
        logger.add(sys.stdout, level=self.log_level)

        self.parser = argparse.ArgumentParser(description=f'{PROGRAM_NAME} v.{PROGRAM_VERSION}')
        self.parser.add_argument("-b", "--begin-date", type=str, help="Дата начала периода анализа формат YYYY-MM-DD")
        self.parser.add_argument("-e", "--end-date", type=str, help="Дата окончания периода анализа формат YYYY-MM-DD")
        self.parser.add_argument("--dont-save-ap", action='store_true', help="Не сохранять адресные планы вместе с отчетом")
        self.parser.add_argument("-s", "--source-file", help="Файл с данными")
        self.parser.add_argument("-r", "--report-file", help="Имя файла с отчетом. Должен иметь расширение .xlsx")
        self.parser.add_argument("--experimental", action='store_true', help="Включить в отчет экспериментальные разделы")
        self.args = self.parser.parse_args()

        if self.args.begin_date is None:
            self.begin_date = datetime.datetime(today_datetime.year, today_datetime.month, 1)
            save_begin_date: str = self.begin_date.strftime('%Y-%m-%d')
            # self.begin_date = self.begin_date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            save_begin_date: str = self.args.begin_date
            split_begin_date: list[int] = list(map(int, save_begin_date.split('-')))
            self.begin_date = datetime.datetime(year=split_begin_date[0], month=split_begin_date[1], day=split_begin_date[2], hour=0, minute=0, second=0)

        # Дата конца анализа
        if self.args.end_date is None:
            self.end_date = datetime.datetime(year=today_datetime.year, month=today_datetime.month, day=calendar.monthrange(today_datetime.year, today_datetime.month)[1], hour=23,
                                              minute=59, second=59, microsecond=99999)
            save_end_date = self.end_date.strftime('%Y-%m-%d')
        else:
            save_end_date = self.args.end_date
            split_end_date: list[int] = list(map(int, save_end_date.split('-')))
            self.end_date = datetime.datetime(year=split_end_date[0], month=split_end_date[1], day=split_end_date[2], hour=23, minute=59, second=59, microsecond=99999)
            # self.end_date = f'{self.args.end_date} {datetime.time(hour=23, minute=59, second=59, microsecond=99999).strftime("%H:%M:%S")}'

        if self.begin_date.year == self.end_date.year:
            self.process_year = [self.begin_date.year]
        else:
            self.process_year = [self.begin_date.year, self.end_date.year]

        self.begin_of_the_year = datetime.datetime(year=self.begin_date.year, month=1, day=1)
        self.end_of_the_year = datetime.datetime(year=self.end_date.year, month=12, day=31, hour=23, minute=59, second=59, microsecond=99999)

        if self.args.source_file is None:
            self.url = Path(f'//megafon.ru/KVK/KRN/Files/TelegrafFiles/ОПРС/!Проекты РЦРП/Блок №3/{self.process_year[0]} год/MDP_24_25.xlsb')
        else:
            if Path(self.args.source_file).is_file():
                self.url = self.args.source_file
            else:
                print(f'{Colors.RED}Файл с данными {self.args.source_file} не найден{Colors.END}')
                sys.exit(130)

        if self.args.report_file is None:
            self.dir_name = Path('//megafon.ru/KVK', 'KRN', 'Files', 'TelegrafFiles', 'ОПРС', '!Проекты РЦРП', 'Блок №3', f'{datetime.datetime.today().year} год', 'Отчеты')
            if self.args.dont_save_ap:
                self.report_file = Path(self.dir_name, f'{datetime.date.today().strftime("%Y%m%d")} Отчет по выполнению мероприятий КФ ({save_begin_date} - {save_end_date}).xlsx')
            else:
                self.report_file = Path(self.dir_name,
                                        f'{datetime.date.today().strftime("%Y%m%d")} Отчет по выполнению мероприятий КФ ({save_begin_date} - {save_end_date}) (АП).xlsx')
        else:
            if os.access(PurePath(self.args.report_file).parents[0], os.W_OK):
                self.report_file = Path(self.args.report_file)
                self.dir_name = PurePath(self.args.report_file).parents[0]
            else:
                print(f'{Colors.RED}Не могу записать файл отчета {self.args.report_file}{Colors.END}')
                sys.exit(140)

        self.sheets = ['Массив', 'mdp_upload_date']

        self.upload_date: pd.DataFrame = pd.DataFrame()
        self.ro_cluster = pd.DataFrame([['Cluster A', 'Белгородская область'],
                                        ['Cluster A', 'Воронежская область'],
                                        ['Cluster A', 'Липецкая область'],
                                        ['Cluster A', 'Тамбовская область'],
                                        ['Cluster B', 'Ростовская область'],
                                        ['Cluster B', 'Сочи'],
                                        ['Cluster C', 'Республика Ингушетия'],
                                        ['Cluster C', 'Республика Северная Осетия-Алания'],
                                        ['Cluster C', 'Ставропольский край'],
                                        ['Cluster C', 'Чеченская республика'],
                                        ['Cluster D', 'Кабардино-Балкарская республика'],
                                        ['Cluster D', 'Карачаево-Черкесская республика'],
                                        ['Cluster D', 'Республика Дагестан'],
                                        ['Cluster E', 'Краснодарский край'],
                                        ['Cluster E', 'Республика Адыгея']],
                                       columns=['RO_CLUSTER', 'RO'])

    def get_data(self) -> Union[pd.DataFrame, dict[str, pd.DataFrame]]:
        try:
            data_update_age = datetime.datetime.now() - datetime.datetime.fromtimestamp(os.stat(self.url).st_mtime)
            if data_update_age > datetime.timedelta(hours=3):
                if input(
                        f'{Colors.RED}Файл {self.url} обновлялся {(data_update_age.days * 24 + data_update_age.seconds / 3600):.2f} часов назад! Хотите продолжить обработку данных (y/N)?{Colors.END}').lower() != 'y':
                    sys.exit(12)
            print(f'Получение данных из файла {Colors.GREEN}"{self.url}"{Colors.END}')
            with open(self.url, 'rb') as f:
                g = io.BytesIO(f.read())
            _df = pd.read_excel(g, sheet_name=self.sheets, engine="calamine")
            g.close()
        except FileNotFoundError as ex:
            raise MyLoggingException(f'Файл {self.url} не существует. Ошибка {ex}')
        except XLRDError:
            try:
                print(f'Try read data from protected file: {Colors.GREEN}"{self.url}"{Colors.END}')
                _df = dict()
                wb = xw.Book(self.url)
                for sh_name in self.sheets:
                    sheet = wb.sheets[wb.sheet_names.index(sh_name)]
                    _df[sh_name] = pd.DataFrame(sheet['A1'].expand().options(pd.DataFrame, chunksize=1_000_000).value).reset_index()
            except XLRDError as err:
                print(f'{Colors.RED}XLRD Error: Ошибка открытия защищенного файла "{self.url}". {err}{Colors.END}')
                sys.exit(140)
            except ValueError as err:
                if "Cannot open two workbooks named" in err.__str__():
                    print(
                        f'{Colors.RED}XLRD Error: Excel не может открыть 2 файла с одним именем, даже сохраненные в разных местах. Закройте окно Excel с файлом "'
                        f'{self.url.name}".{Colors.END}')
                else:
                    print(f'{Colors.RED}XLRD Error: {err}{Colors.END}')
                sys.exit(140)
            except Exception as ex:
                raise MyLoggingException(f'Ошибка при получении данных: {ex}')
        return _df

    @staticmethod
    def make_date_mask(_df: pd.DataFrame, column_name: str, _begin_date: datetime, _end_date: datetime) -> bool:
        """
        Формирует простую маску для анализа по датам
        :param _df: исходные данные для формирования маски
        :param column_name: имя колонки, по которой формируется маска
        :param _begin_date: дата начала периода
        :param _end_date: дата окончания периода
        :return: возвращает логическую маску, для дальнейшего применения в анализе
        """

        _result = (_df[column_name] >= _begin_date) & (_df[column_name] <= _end_date)
        return _result

    def make_report(self, _df: pd.DataFrame, _dfo: pd.DataFrame = None, divide_prognosis: bool = False, add_spec: bool = False) -> pd.DataFrame:
        """
        Собирает сводный отчет из исходных данных
        :param _df: данные для анализа
        :param _dfo: данные по обязательствам
        :param divide_prognosis: признак (False/True), который позволяет разделить данные прогноза на работы ПО и работы своими силами
        :param add_spec: позволяет добавить в таблицу данные по комплектам 48 заявок и подаче в НП
        :return: возвращает сформированную сводную таблицу
        """

        delta_char = f'{chr(0x0394)}'
        _df[['PROGNOZ_DATE', 'PLAN_DATE_END']].apply(pd.to_datetime)

        rename_columns = {
            'RO_CLUSTER': 'Кластер',
            'RO': 'Регион',
            'PLAN_DATE_END': 'Оперплан',
            'PROGNOZ_DATE': 'Прогноз периода',
            'CUMM_PROGNOZ_DATE': 'Прогноз',
            'Выдача оборудования': 'Выдача',
            'FORWARD_VIDACHA': 'Выдача вперёд',
            'CHECK_FACT': 'Факт',
            '83_done': 'Выдача (по 83)',
            'PROGNOZ_DATE_PO': 'Прогноз периода. ПО строительства',
            'PROGNOZ_DATE_SELF': 'Прогноз периода. ПО ПЭ',
            'Комплект 48-х': 'Специф.',
            'НП': 'НП',

        }
        mask_cumm_plan_date = self.make_date_mask(_df, 'PLAN_DATE_END', self.begin_of_the_year, self.end_date)
        mask_cumm_prognoz_date = self.make_date_mask(_df, 'PROGNOZ_DATE', self.begin_of_the_year, self.end_date)
        mask_prognoz_date = self.make_date_mask(_df, 'PROGNOZ_DATE', self.begin_date, self.end_date)
        mask_fact_date = self.make_date_mask(_df, 'MIN_DATE_FACT', self.begin_of_the_year, self.end_date)
        # mask_vidacha_date = self.make_date_mask(_df, 'PROGNOZ_DATE', self.begin_of_the_year, self.end_of_the_year)
        mask_vidacha_date = self.make_date_mask(_df, 'PROGNOZ_DATE', self.begin_of_the_year, self.end_date)
        mask_vidacha_date_forward = self.make_date_mask(_df, 'PROGNOZ_DATE', self.end_date + datetime.timedelta(seconds=2), self.end_of_the_year)
        mask_check_fact = (_df['CHECK_FACT'] == 1)
        mask_check_plan = (_df['CHECK_PLAN'] == "Да")
        mask_check_vidacha = (_df['Выдача оборудования'] == 1)
        # mask_check_vidacha = (_df['83_done'] == 1)
        mask_48_complete = (_df['Комплект 48-х'] == 1)
        mask_48_complete_date = self.make_date_mask(_df, 'PROGNOZ_DATE', self.begin_of_the_year, self.end_date)
        mask_np = (_df['НП'] == 1)
        mask_np_date = self.make_date_mask(_df, 'PROGNOZ_DATE', self.begin_of_the_year, self.end_date)

        # TODO: Временно до объединения программ 2024 и 2025
        mask_exclude_done_2024 = _df['MIN_DATE_FACT'] < datetime.datetime(2025, 1, 1)

        # TODO:
        mask_include_2025_done = _df['MIN_DATE_FACT'] >= datetime.datetime(2025, 1, 1)
        mask_addition = (mask_include_2025_done & mask_check_plan & mask_check_fact)

        logger.debug(_df[mask_prognoz_date])
        # df_cumm_plan = _df[(mask_cumm_plan_date | mask_addition) & ~mask_exclude_done_2024].groupby(['RO_CLUSTER', 'RO']).agg({'PLAN_DATE_END': 'count',}).reset_index()
        df_cumm_plan = _df[mask_cumm_plan_date & ~mask_exclude_done_2024].groupby(['RO_CLUSTER', 'RO']).agg({'PLAN_DATE_END': 'count', }).reset_index()
        df_cumm_prognoz = _df[mask_cumm_prognoz_date & ~mask_exclude_done_2024].groupby(['RO_CLUSTER', 'RO']).agg({'PROGNOZ_DATE': 'count', }).rename(columns={'PROGNOZ_DATE': 'CUMM_PROGNOZ_DATE'}).reset_index()
        if divide_prognosis:
            mask_po_self_do = _df['PO'] == 'Работы своими силами'
            df_prognoz_po = _df[mask_prognoz_date & ~mask_po_self_do & ~mask_exclude_done_2024].groupby(
                ['RO_CLUSTER', 'RO']).agg(
                {'PROGNOZ_DATE': 'count', }).rename(
                columns={'PROGNOZ_DATE': 'PROGNOZ_DATE_PO'}).reset_index()
            df_prognoz_self_do = _df[mask_prognoz_date & mask_po_self_do & ~mask_exclude_done_2024].groupby(
                ['RO_CLUSTER', 'RO']).agg(
                {'PROGNOZ_DATE': 'count', }).rename(
                columns={'PROGNOZ_DATE': 'PROGNOZ_DATE_SELF'}).reset_index()
        else:
            df_prognoz = _df[mask_prognoz_date & ~mask_exclude_done_2024].groupby(['RO_CLUSTER', 'RO']).agg({'PROGNOZ_DATE': 'count', }).reset_index()

        df_fact = _df[mask_fact_date & mask_check_fact].groupby(['RO_CLUSTER', 'RO']).agg({'CHECK_FACT': 'count', }).reset_index()
        df_vidacha = _df[mask_vidacha_date & mask_check_vidacha].groupby(['RO_CLUSTER', 'RO']).agg({'Выдача оборудования': 'count', }).reset_index()
        df_vidacha_forward = _df[mask_vidacha_date_forward & mask_check_vidacha].groupby(['RO_CLUSTER', 'RO']).agg({'Выдача оборудования': 'count', }).rename(
            columns={'Выдача оборудования': 'FORWARD_VIDACHA'}).reset_index()

        df_48_complete = _df[mask_48_complete_date & mask_48_complete].groupby(['RO_CLUSTER', 'RO']).agg({'Комплект 48-х': 'count', }).reset_index()
        df_np = _df[mask_np_date & mask_np].groupby(['RO_CLUSTER', 'RO']).agg({'НП': 'count', }).reset_index()

        # Список данных для объединения
        if divide_prognosis:
            if add_spec:
                data_frames = [df_cumm_plan, df_cumm_prognoz, df_fact, df_vidacha, df_vidacha_forward, df_prognoz_po, df_prognoz_self_do, df_48_complete, df_np]
            else:
                data_frames = [df_cumm_plan, df_cumm_prognoz, df_fact, df_vidacha, df_vidacha_forward, df_prognoz_po, df_prognoz_self_do]
        else:
            if add_spec:
                data_frames = [df_cumm_plan, df_cumm_prognoz, df_fact, df_vidacha, df_vidacha_forward, df_prognoz, df_48_complete, df_np]
            else:
                data_frames = [df_cumm_plan, df_cumm_prognoz, df_fact, df_vidacha, df_vidacha_forward, df_prognoz]

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            # Объединение с .merge и использование functools.reduce
            df_merged = reduce(lambda left, right: pd.merge(left, right, how='outer', sort=True, on=['RO_CLUSTER', 'RO']), data_frames).fillna(value=0).sort_values(by='RO').rename(
                columns=rename_columns)

            # Еще один вариант с .merge "в лоб"
            # df_merged = pd.merge(df_cumm_plan,
            #                pd.merge(df_prognoz, pd.merge(df_vidacha, df_fact, how='outer', sort=True, on=['RO_CLUSTER', 'RO']), how='outer', sort=True, on=['RO_CLUSTER', 'RO']),
            #                how='outer', sort=True, on=['RO_CLUSTER', 'RO']).fillna(value=0).sort_values(by='RO').rename(columns=rename_columns)

            # Объединение с .join
            # data_frames = [data_frame.set_index(['RO_CLUSTER', 'RO']) for data_frame in data_frames]
            # df_merged = data_frames[0].join(data_frames[1:], how='outer', sort=True).reset_index().fillna(value=0).sort_values(by='RO').rename(columns=rename_columns)

            # Добавляем подсчет суммы в строку ИТОГО:
            df_merged[delta_char] = df_merged[rename_columns['CHECK_FACT']] - df_merged[rename_columns['PLAN_DATE_END']]
            df_merged.loc["total"] = df_merged.sum(numeric_only=True)
            df_merged.at["total", 'Регион'] = "ИТОГО:"

            # Удаляем кластеры из итоговой таблицы
            if divide_prognosis:
                if add_spec:
                    df_merged = df_merged[[rename_columns['RO'],
                                           rename_columns['PLAN_DATE_END'],
                                           rename_columns['CUMM_PROGNOZ_DATE'],
                                           rename_columns['Комплект 48-х'],
                                           rename_columns['НП'],
                                           rename_columns['Выдача оборудования'],
                                           rename_columns['FORWARD_VIDACHA'],
                                           rename_columns['CHECK_FACT'],
                                           rename_columns['PROGNOZ_DATE_PO'],
                                           rename_columns['PROGNOZ_DATE_SELF'],
                                           delta_char]]
                else:
                    df_merged = df_merged[[rename_columns['RO'],
                                           rename_columns['PLAN_DATE_END'],
                                           rename_columns['CUMM_PROGNOZ_DATE'],
                                           rename_columns['Выдача оборудования'],
                                           rename_columns['FORWARD_VIDACHA'],
                                           rename_columns['CHECK_FACT'],
                                           rename_columns['PROGNOZ_DATE_PO'],
                                           rename_columns['PROGNOZ_DATE_SELF'],
                                           delta_char]]
            else:
                if add_spec:
                    df_merged = df_merged[[rename_columns['RO'],
                                           rename_columns['PLAN_DATE_END'],
                                           rename_columns['CUMM_PROGNOZ_DATE'],
                                           rename_columns['Комплект 48-х'],
                                           rename_columns['НП'],
                                           rename_columns['Выдача оборудования'],
                                           rename_columns['FORWARD_VIDACHA'],
                                           rename_columns['CHECK_FACT'],
                                           rename_columns['PROGNOZ_DATE'],
                                           delta_char]]
                else:
                    df_merged = df_merged[[rename_columns['RO'],
                                           rename_columns['PLAN_DATE_END'],
                                           rename_columns['CUMM_PROGNOZ_DATE'],
                                           rename_columns['Выдача оборудования'],
                                           rename_columns['FORWARD_VIDACHA'],
                                           rename_columns['CHECK_FACT'],
                                           rename_columns['PROGNOZ_DATE'],
                                           delta_char]]
        return df_merged

    def report_kpi(self, df_kpi: pd.DataFrame) -> FormattedWorkbook:
        report_sheets = {
            'Всего БС': 'all_bs_report',
            'Новые БС': 'new_bs_report',
            'Существующие БС': 'exist_bs_report',
            'РРЛ': 'rrl_report',
            'Энерго': 'energy_report',
            'Энерго ПО строительства': 'energy_report_po',
            'Энерго ПО ПЭ': 'energy_report_self_do',
            'Климатика': 'climate_report',
            'Климатика ПО строительства': 'climate_report_po',
            'Климатика ПО ПЭ': 'climate_report_self_do',
            'АП БС': 'ap_all_bs',
            'АП РРЛ': 'ap_rrl',
            'АП Энерго': 'ap_energy',
            'АП Климатика': 'ap_climate',
            'Дата выгрузки данных': 'upload_date',
            'IPBH': 'ipbh_report',
            'ВОЛС': 'vols_report',
            'АП IPBH': 'ap_ipbh',
            'АП ВОЛС': 'ap_vols',
            'АКБ': 'akb_report',
        }

        wb = FormattedWorkbook(logging_level=self.log_level)

        if not self.upload_date.empty:
            name_of_upload = 'Дата выгрузки данных'
            self.upload_date = self.upload_date.rename(columns={'DATE_UPLOAD': name_of_upload})
            print(f'Создаем лист отчета: {Colors.GREEN}"{name_of_upload}"{Colors.END}')
            wb.excel_format_table(self.upload_date, name_of_upload, report_sheets[name_of_upload])

        rename_columns = {
            'ID_ESUP': 'ЕСУП ID',
            'SAP_EVT': 'SAP EVT',
            'BP_ESUP': 'Бизнес процесс',
            'RO': 'Региональное отделение',
            'CHECK_NEW_PLAN': 'Новая/Существующая',
            'NAZ': 'Наименование',
            'PLAN_DATE_END': 'Плановая дата',
            'PROGNOZ_DATE': 'Прогнозная дата',
            'PROGNOZ_COMMENT': 'Комментарий к прогнозной дате',
            'RS_2023': 'RAN Sharing 2023',
            'MIN_DATE_FACT': 'Мин. дата запуска',
            'MAX_DATE_FACT': 'Макс. дата запуска',
            'PROGRAM': 'Программа',
            'CHECK_FACT': 'Факт запуска',
            'RO_CLUSTER': 'Кластер',
            'build_priority': 'Приоритет',
            'Выдача оборудования': 'Выдача оборудования',
            '83_done': 'Выдача по 83',
        }

        report_columns = [
            'ID_ESUP',
            'BP_ESUP',
            'PROGRAM',
            'CHECK_PLAN',
            'CHECK_FACT',
            'RO',
            'RO_CLUSTER',
            'NAZ',
            'CHECK_NEW_PLAN',
            'PO',
            'PLAN_DATE_END',
            'PROGNOZ_DATE',
            'PROGNOZ_COMMENT',
            'MIN_DATE_FACT',
            'Выдача оборудования',
            'Комплект 48-х',
            'НП',
        ]

        mask_rrl_build = df_kpi['BP_ESUP'] == 'Строительство РРЛ'
        mask_rrl_rec = df_kpi['BP_ESUP'] == 'Переоборудование РРЛ'
        mask_bs_build = df_kpi['BP_ESUP'] == 'Строительство БС/АМС'
        mask_bs_rec = df_kpi['BP_ESUP'] == 'Переоборудование БС'
        mask_bs_rs_on = df_kpi['BP_ESUP'] == 'БС_Включение RAN Sharing'
        mask_energo = df_kpi['BP_ESUP'] == 'Модернизация энергоснабжения'
        mask_climate = df_kpi['BP_ESUP'] == 'Модернизация климатического оборудования'
        mask_ipbh = df_kpi['BP_ESUP'] == 'Ввод/модернизация/демонтаж элемента ТС - IPBH'
        mask_vols = df_kpi['BP_ESUP'] == 'Строительство ВОЛС (городская)'
        mask_akb = ((df_kpi['BP_ESUP'] == 'Модернизация энергоснабжения') & (df_kpi['PROGRAM'] == 'КФ. Base Case Эксплуатации – АКБ Волна 1. 2025'))

        mask_po_self_do = df_kpi['PO'] == 'Работы своими силами'

        # Убрал, в связи с изменение методики KPI в 2024 году
        # mask_bs_pico = df_kpi['BP_ESUP'] == 'Pico Cell_Включение'
        # mask_bs_dem = df_kpi['BP_ESUP'] == 'Демонтаж БС/АМС'

        if self.process_year.__len__() == 2:
            mask_plan_year = (df_kpi['PLAN_YEAR'] == self.process_year[0]) | (df_kpi['PLAN_YEAR'] == self.process_year[1])
        else:
            mask_plan_year = df_kpi['PLAN_YEAR'] == self.process_year[0]

        mask_new_bs = df_kpi['CHECK_NEW_PLAN'] == 'Новая'
        # mask_check_plan = (df_kpi['CHECK_PLAN'] == 'Да') | (df_kpi['CHECK_FACT'] == 1)
        mask_check_plan = (df_kpi['CHECK_PLAN'] == 'Да')

        # mask_2024_2023_boost = df_kpi['PROGRAM'] == "КФ. Развитие регионов_Ускоренные запуски 2024. 2023"
        # mask_check_plan = mask_check_plan | mask_2024_2023_boost

        # df_all_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_rs_on) & mask_plan_year][report_columns]
        df_all_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_rs_on)][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Всего БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_all_bs), 'Всего БС', report_sheets['Всего БС'])

        # df_new_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_rs_on) & mask_new_bs & mask_plan_year][report_columns]
        df_new_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_rs_on) & mask_new_bs][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Новые БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_new_bs), 'Новые БС', report_sheets['Новые БС'])

        # df_exist_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_rs_on) & ~mask_new_bs & mask_plan_year][report_columns]
        df_exist_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_rs_on) & ~mask_new_bs][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Существующие БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_exist_bs), 'Существующие БС', report_sheets['Существующие БС'])

        df_rrl = df_kpi[mask_check_plan & (mask_rrl_build | mask_rrl_rec)][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"РРЛ"{Colors.END}')
        wb.excel_format_table(self.make_report(df_rrl), 'РРЛ', report_sheets['РРЛ'])

        df_energy = df_kpi[mask_check_plan & mask_energo][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Энерго"{Colors.END}')
        wb.excel_format_table(self.make_report(df_energy, divide_prognosis=False), 'Энерго', report_sheets['Энерго'])

        df_climate = df_kpi[mask_check_plan & mask_climate][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Климатика"{Colors.END}')
        wb.excel_format_table(self.make_report(df_climate, divide_prognosis=False), 'Климатика', report_sheets['Климатика'])

        if self.args.experimental:
            df_ipbh = df_kpi[mask_check_plan & mask_ipbh][report_columns]
            print(f'Создаем лист отчета: {Colors.GREEN}"IPBH"{Colors.END}')
            wb.excel_format_table(self.make_report(df_ipbh), 'IPBH', report_sheets['IPBH'])

            df_vols = df_kpi[mask_check_plan & mask_vols][report_columns]
            print(f'Создаем лист отчета: {Colors.GREEN}"ВОЛС"{Colors.END}')
            wb.excel_format_table(self.make_report(df_vols), 'ВОЛС', report_sheets['ВОЛС'])

            df_akb = df_kpi[mask_check_plan & mask_akb][report_columns]
            print(f'Создаем лист отчета: {Colors.GREEN}"АКБ"{Colors.END}')
            wb.excel_format_table(self.make_report(df_akb, add_spec=True), 'АКБ', report_sheets['АКБ'])

        if not self.args.dont_save_ap:
            # Сохраняем АП
            ap_list = [["АП БС", df_all_bs], ["АП РРЛ", df_rrl], ["АП Энерго", df_energy], ["АП Климатика", df_climate]]
            if self.args.experimental:
                ap_list.append(["АП IPBH", df_ipbh])
                ap_list.append(["АП ВОЛС", df_vols])
            for sheet_name, df_name in ap_list:
                mask_prognoz_date = self.make_date_mask(df_name, 'PROGNOZ_DATE', self.begin_date, self.end_date)
                _df = df_name[mask_prognoz_date].sort_values(by=['RO']).rename(columns=rename_columns)
                if not _df.empty:
                    print(f'Создаем лист отчета: {Colors.GREEN}"{sheet_name}"{Colors.END}')
                    wb.excel_format_table(_df, sheet_name, report_sheets[sheet_name])
        return wb

    def save_report(self, wb: FormattedWorkbook) -> None:
        if len(wb.worksheets) != 0:
            if Path(self.report_file).is_file():
                try:
                    print(f'Удаляем старый файл отчета {Colors.GREEN}"{self.report_file}"{Colors.END}')
                    os.remove(self.report_file)
                except Exception as ex:
                    raise MyLoggingException(f'Не могу удалить файл отчета "{self.report_file}". Ошибка: {ex}')
            try:
                logger.info(f'Удаляем лист {wb.active}')
                wb.remove(wb.active)
                print(f'Сохраняем отформатированный файл отчета: {Colors.GREEN}"{self.report_file}"{Colors.END}')
                wb.save(self.report_file)
            except Exception as ex:
                raise MyLoggingException(f'Не могу сохранить файл отчета "{self.report_file}". Ошибка: {ex}')


def main():
    locale.setlocale(locale.LC_ALL, '')
    wr = WeeklyReport()
    print(f'{Colors.DARKCYAN}{datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}:{Colors.END} {PROGRAM_NAME} v.{PROGRAM_VERSION}')
    df = wr.get_data()
    if df.__len__() > 1:
        wr.upload_date = df[wr.sheets[1]]
    work_book = wr.report_kpi(df[wr.sheets[0]])
    wr.save_report(work_book)


if __name__ == '__main__':
    main()
