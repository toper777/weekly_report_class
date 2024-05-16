import argparse
import datetime
import io
import locale
import os
import sys
import calendar
import warnings
from functools import reduce
from pathlib import Path, PurePath
from typing import Union

import pandas as pd
from loguru import logger
from xlrd import XLRDError
import xlwings as xw

from Colors import Colors
from FormattedWorkbook import FormattedWorkbook
from MyLoggingException import MyLoggingException


class WeeklyReport:
    def __init__(self):
        self.program_name = Path(__file__).stem
        self.program_version = "0.3.4"
        self.log_level = 'ERROR'

        today_datetime = datetime.datetime.now()

        logger.remove()
        logger.add(sys.stdout, level=self.log_level)

        self.parser = argparse.ArgumentParser(description=f'{self.program_name} v.{self.program_version}')
        self.parser.add_argument("-b", "--begin-date", type=str, help="Дата начала периода анализа формат YYYY-MM-DD")
        self.parser.add_argument("-e", "--end-date", type=str, help="Дата окончания периода анализа формат YYYY-MM-DD")
        self.parser.add_argument("--dont-save-ap", action='store_true', help="Не сохранять адресные планы вместе с отчетом")
        self.parser.add_argument("-s", "--source-file", help="Файл с данными")
        self.parser.add_argument("-r", "--report-file", help="Имя файла с отчетом. Должен иметь расширение .xlsx")
        self.parser.add_argument("-o", "--obligations-file", help="Файла с обязательствами")
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

        if self.args.source_file is None:
            self.url = Path(f'//megafon.ru/KVK/KRN/Files/TelegrafFiles/ОПРС/!Проекты РЦРП/Блок №3/{self.process_year[0]} год/MDP_23_24.xlsb')
        else:
            if Path(self.args.source_file).is_file():
                self.url = self.args.source_file
            else:
                print(f'{Colors.RED}Файл с данными {self.args.source_file} не найден{Colors.END}')
                sys.exit(130)

        if self.args.obligations_file is None:
            self.obligations_file = Path(
                f'//megafon.ru/KVK/KRN/Files/TelegrafFiles/ОПРС/!Проекты РЦРП/Блок №3/{self.process_year[0]} год/Обязательства КФ {self.process_year[0]}.xlsx')
        else:
            if Path(self.args.obligation_file).is_file():
                self.obligations_file = self.args.obligation_file
            else:
                print(f'{Colors.RED}Файл с данными {self.args.obligation_file} не найден{Colors.END}')
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

        self.end_of_the_year = datetime.datetime(year=self.end_date.year, month=12, day=31, hour=23, minute=59, second=59, microsecond=99999)
        self.sheets = ['Массив', 'mdp_upload_date']
        self.obligations_sheets = [f"Обязательства {month}" for month in pd.date_range(self.begin_date, self.end_date, freq='MS').strftime("%m.%Y").tolist()]
        self.not_done_file = Path(self.dir_name, 'Риски ВОЛС.xlsx')
        self.upload_date: pd.DataFrame = pd.DataFrame()
        self.obligations = None
        self.dfo = dict()
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
        self.get_obligations()
        return _df

    def get_obligations(self):
        bp_list = ['Новые БС', 'Существующие БС', 'РРЛ', 'Энергетика', 'Климатика']
        obligations_string = 'Обязательства регионов'
        names_indexes = ['RO_CLUSTER', 'RO']
        _obligations = None

        try:
            print(f'Получение данных из файла обязательств {Colors.GREEN}"{self.obligations_file}"{Colors.END}')
            with open(self.obligations_file, 'rb') as f:
                g = io.BytesIO(f.read())
                dfo = pd.read_excel(g, sheet_name=self.obligations_sheets, engine="calamine")

            # Начинаем обработку файла обязательств. Присваиваем 1-ый лист
            dfo[self.obligations_sheets[0]].rename(columns={dfo[self.obligations_sheets[0]].columns[0]: 'RO'}, inplace=True)
            _obligations = dfo[self.obligations_sheets[0]]

            # Объединяем обязательства периода в суммарную таблицу
            for dfx in self.obligations_sheets[1:]:
                dfo[dfx].rename(columns={dfo[dfx].columns[0]: 'RO'}, inplace=True)
                _obligations = _obligations.set_index('RO').add(dfo[dfx].set_index('RO'), fill_value=0).reset_index()

            # Формируем таблицы обязательства по Бизнес-процессам
            _obligations = _obligations[:-1]  # Отрезаем строку ИТОГО:
            _obligations = _obligations.merge(self.ro_cluster, on='RO', how='outer')  # Добавляем кластера для дальнейшей обработки

            self.obligations = {'DATA': _obligations}

            # Формируем объекты обязательств
            for bp_name in bp_list:
                indexes = list((*names_indexes, bp_name))
                self.obligations[bp_name] = _obligations[indexes].rename(columns={bp_name: obligations_string})

            # Формируем Обязательства Всего БС (Новые + Существующие)
            self.obligations['Всего БС'] = self.obligations['Новые БС'].set_index(names_indexes).add(self.obligations['Существующие БС'].set_index(names_indexes), fill_value=0).reset_index()

        except FileNotFoundError as ex:
            raise MyLoggingException(f'Файл {self.url} не существует. Ошибка {ex}')

    @staticmethod
    def make_date_mask(_df: pd.DataFrame, column_name: str, _begin_date: datetime, _end_date: datetime) -> pd.Series(bool):
        _result = (_df[column_name] >= _begin_date) & (_df[column_name] <= _end_date)
        return _result

    def make_report(self, _df: pd.DataFrame, _dfo: pd.DataFrame) -> pd.DataFrame:
        delta_char = f'{chr(0x0394)}'
        _df[['PROGNOZ_DATE', 'PLAN_DATE_END']].apply(pd.to_datetime)

        rename_columns = {
            'RO_CLUSTER': 'Кластер',
            'RO': 'Регион',
            'PLAN_DATE_END': 'MDP План',
            'PROGNOZ_DATE': 'Прогноз',
            'VIDACHA': 'Выдача',
            'CHECK_FACT': 'Факт',
            '83_done': 'Выдача (по 83)',
        }
        mask_plan_date = self.make_date_mask(_df, 'PLAN_DATE_END', self.begin_date, self.end_date)
        mask_prognoz_date = self.make_date_mask(_df, 'PROGNOZ_DATE', self.begin_date, self.end_date)
        mask_fact_date = self.make_date_mask(_df, 'MIN_DATE_FACT', self.begin_date, self.end_date)
        mask_vidacha_date = self.make_date_mask(_df, 'PROGNOZ_DATE', self.begin_date, self.end_of_the_year)
        mask_check_fact = (_df['CHECK_FACT'] == 1)
        mask_check_vidacha = (_df['VIDACHA'] == 1)
        # mask_check_vidacha = (_df['83_done'] == 1)

        logger.debug(_df[mask_prognoz_date])
        df_plan = _df[mask_plan_date].groupby(['RO_CLUSTER', 'RO']).agg({'PLAN_DATE_END': 'count', }).reset_index()
        df_prognoz = _df[mask_prognoz_date].groupby(['RO_CLUSTER', 'RO']).agg({'PROGNOZ_DATE': 'count', }).reset_index()
        df_fact = _df[mask_fact_date & mask_check_fact].groupby(['RO_CLUSTER', 'RO']).agg({'CHECK_FACT': 'count', }).reset_index()
        df_vidacha = _df[mask_vidacha_date & mask_check_vidacha].groupby(['RO_CLUSTER', 'RO']).agg({'VIDACHA': 'count', }).reset_index()
        # df_vidacha = _df[mask_vidacha_date & mask_check_vidacha].groupby(['RO_CLUSTER', 'RO']).agg({'83_done': 'count', }).reset_index()

        # Список данных для объединения
        data_frames = [df_plan, _dfo, df_prognoz, df_fact, df_vidacha]

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            # Объединение с .merge и использование functools.reduce
            # df_merged = reduce(lambda left, right: pd.merge(left, right, how='outer', sort=True, on=['RO_CLUSTER', 'RO']), data_frames).fillna(value=0).sort_values(by='RO').rename(columns=rename_columns)

            # Еще один вариант с .merge "в лоб"
            # df_merged = pd.merge(df_plan,
            #                pd.merge(df_prognoz, pd.merge(df_vidacha, df_fact, how='outer', sort=True, on=['RO_CLUSTER', 'RO']), how='outer', sort=True, on=['RO_CLUSTER', 'RO']),
            #                how='outer', sort=True, on=['RO_CLUSTER', 'RO']).fillna(value=0).sort_values(by='RO').rename(columns=rename_columns)

            # Объединение с .join
            data_frames = [data_frame.set_index(['RO_CLUSTER', 'RO']) for data_frame in data_frames]
            df_merged = data_frames[0].join(data_frames[1:], how='outer', sort=True).reset_index().fillna(value=0).sort_values(by='RO').rename(columns=rename_columns)

            # Добавляем подсчет суммы в строку ИТОГО:
            df_merged[delta_char] = df_merged['Факт'] - df_merged['Прогноз']
            df_merged.loc["total"] = df_merged.sum(numeric_only=True)
            df_merged.at["total", 'Регион'] = "ИТОГО:"

            # Удаляем кластеры из итоговой таблицы
            df_merged = df_merged[[rename_columns['RO'],
                                   rename_columns['PLAN_DATE_END'],
                                   'Обязательства регионов',
                                   rename_columns['PROGNOZ_DATE'],
                                   rename_columns['VIDACHA'],
                                   rename_columns['CHECK_FACT'],
                                   delta_char]]
        return df_merged

    def report_kpi(self, df_kpi: pd.DataFrame) -> FormattedWorkbook:
        report_sheets = {
            'Всего БС': 'all_bs_report',
            'Новые БС': 'new_bs_report',
            'Существующие БС': 'exist_bs_report',
            'РРЛ': 'rrl_report',
            'Энерго': 'energy_report',
            'Климатика': 'climate_report',
            'АП БС': 'ap_all_bs',
            'АП РРЛ': 'ap_rrl',
            'АП Энерго': 'ap_energy',
            'АП Климатика': 'ap_climate',
            'Дата выгрузки данных': 'upload_date'
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
            'VIDACHA': 'Выдача оборудования',
            '83_done': 'Выдача по 83',
        }

        report_columns = [
            'ID_ESUP',
            'BP_ESUP',
            'PROGRAM',
            'CHECK_FACT',
            'RO',
            'RO_CLUSTER',
            'NAZ',
            'CHECK_NEW_PLAN',
            'PLAN_DATE_END',
            'PROGNOZ_DATE',
            'PROGNOZ_COMMENT',
            'RS_2023',
            'VIP',
            'build_priority',
            'MIN_DATE_FACT',
            'VIDACHA',
            '83_done',
        ]

        mask_rrl_build = df_kpi['BP_ESUP'] == 'Строительство РРЛ'
        mask_rrl_rec = df_kpi['BP_ESUP'] == 'Переоборудование РРЛ'
        mask_bs_build = df_kpi['BP_ESUP'] == 'Строительство БС/АМС'
        mask_bs_rec = df_kpi['BP_ESUP'] == 'Переоборудование БС'
        mask_bs_rs_on = df_kpi['BP_ESUP'] == 'БС_Включение RAN Sharing'
        mask_energo = df_kpi['BP_ESUP'] == 'Модернизация энергоснабжения'
        mask_climate = df_kpi['BP_ESUP'] == 'Модернизация климатического оборудования'

        # Убрал, в связи с изменение методики KPI в 2024 году
        # mask_bs_pico = df_kpi['BP_ESUP'] == 'Pico Cell_Включение'
        # mask_bs_dem = df_kpi['BP_ESUP'] == 'Демонтаж БС/АМС'

        if self.process_year.__len__() == 2:
            mask_plan_year = (df_kpi['PLAN_YEAR'] == self.process_year[0]) | (df_kpi['PLAN_YEAR'] == self.process_year[1])
        else:
            mask_plan_year = df_kpi['PLAN_YEAR'] == self.process_year[0]

        mask_new_bs = df_kpi['CHECK_NEW_PLAN'] == 'Новая'
        mask_check_plan = df_kpi['CHECK_PLAN'] == 'Да'

        mask_2024_2023_boost = df_kpi['PROGRAM'] == "КФ. Развитие регионов_Ускоренные запуски 2024. 2023"
        mask_check_plan = mask_check_plan | mask_2024_2023_boost

        df_all_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_rs_on) & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Всего БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_all_bs, self.obligations["Всего БС"]), 'Всего БС', report_sheets['Всего БС'])

        df_new_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_rs_on) & mask_new_bs & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Новые БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_new_bs, self.obligations["Новые БС"]), 'Новые БС', report_sheets['Новые БС'])

        df_exist_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_rs_on) & ~mask_new_bs & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Существующие БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_exist_bs, self.obligations["Существующие БС"]), 'Существующие БС', report_sheets['Существующие БС'])

        df_rrl = df_kpi[mask_check_plan & (mask_rrl_build | mask_rrl_rec) & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"РРЛ"{Colors.END}')
        wb.excel_format_table(self.make_report(df_rrl, self.obligations["РРЛ"]), 'РРЛ', report_sheets['РРЛ'])

        df_energy = df_kpi[mask_check_plan & mask_energo & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Энерго"{Colors.END}')
        wb.excel_format_table(self.make_report(df_energy, self.obligations["Энергетика"]), 'Энерго', report_sheets['Энерго'])

        df_climate = df_kpi[mask_check_plan & mask_climate & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Климатика"{Colors.END}')
        wb.excel_format_table(self.make_report(df_climate, self.obligations["Климатика"]), 'Климатика', report_sheets['Климатика'])

        if not self.args.dont_save_ap:
            # Сохраняем АП
            for sheet_name, df_name in [["АП БС", df_all_bs], ["АП РРЛ", df_rrl], ["АП Энерго", df_energy], ["АП Климатика", df_climate]]:
                mask_prognoz_date = self.make_date_mask(df_name, 'PROGNOZ_DATE', self.begin_date, self.end_date)
                _df = df_name[mask_prognoz_date].sort_values(by=['build_priority', 'RO']).rename(columns=rename_columns)
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

    def _read_not_done(self):
        try:
            print(f'Получение данных из файла {Colors.GREEN}"{self.not_done_file}"{Colors.END}')
            _df_n_d = pd.read_excel(self.not_done_file, sheet_name=self.sheets)
            return _df_n_d
        except FileNotFoundError as ex:
            raise MyLoggingException(f'Файл {self.not_done_file} не существует. Ошибка {ex}')
        except Exception as ex:
            raise MyLoggingException(f'Ошибка при получении данных: {ex}')


if __name__ == '__main__':
    locale.setlocale(locale.LC_ALL, '')
    wr = WeeklyReport()
    print(f'{Colors.DARKCYAN}{datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}:{Colors.END} {wr.program_name} v.{wr.program_version}')
    df = wr.get_data()
    if df.__len__() > 1:
        wr.upload_date = df[wr.sheets[1]]
    work_book = wr.report_kpi(df[wr.sheets[0]])
    wr.save_report(work_book)
