import argparse
import datetime
import io
import locale
import os
import sys
import calendar
from pathlib import Path, PurePath

import pandas as pd
from loguru import logger

from Colors import Colors
from FormattedWorkbook import FormattedWorkbook
from MyLoggingException import MyLoggingException


class WeeklyReport:
    def __init__(self):
        self.program_name = Path(__file__).stem
        self.program_version = "0.2.11"
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
        self.args = self.parser.parse_args()

        if self.args.begin_date is None:
            self.begin_date = datetime.datetime(today_datetime.year, today_datetime.month, 1)
            save_begin_date: str = self.begin_date.strftime('%Y-%m-%d')
            # self.begin_date = self.begin_date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            save_begin_date: str = self.args.begin_date
            split_begin_date: list[int] = list(map(int, save_begin_date.split('-')))
            self.begin_date = datetime.datetime(year=split_begin_date[0],  month=split_begin_date[1], day=split_begin_date[2], hour=0, minute=0, second=0)

        # Дата конца анализа
        if self.args.end_date is None:
            self.end_date = datetime.datetime(year=today_datetime.year, month=today_datetime.month, day=calendar.monthrange(today_datetime.year, today_datetime.month)[1], hour=23, minute=59, second=59, microsecond=99999)
            save_end_date = self.end_date.strftime('%Y-%m-%d')
        else:
            save_end_date = self.args.end_date
            split_end_date: list[int] = list(map(int, save_end_date.split('-')))
            self.end_date = datetime.datetime(year=split_end_date[0],  month=split_end_date[1], day=split_end_date[2], hour=23, minute=59, second=59, microsecond=99999)
            # self.end_date = f'{self.args.end_date} {datetime.time(hour=23, minute=59, second=59, microsecond=99999).strftime("%H:%M:%S")}'

        if self.begin_date.year == self.end_date.year:
            self.process_year = [self.begin_date.year]
        else:
            self.process_year = [self.begin_date.year, self.end_date.year]

        if self.args.source_file is None:
            self.url = Path('//megafon.ru/KVK/KRN/Files/TelegrafFiles/ОПРС/!Проекты РЦРП/Блок №3/2023 год/MDP_23_24.xlsm')
        else:
            if Path(self.args.source_file).is_file():
                self.url = self.args.source_file
            else:
                print(f'{Colors.RED}Файл с данные {self.args.source_file} не найден{Colors.END}')
                sys.exit(130)

        if self.args.report_file is None:
            self.dir_name = Path('//megafon.ru/KVK', 'KRN', 'Files', 'TelegrafFiles', 'ОПРС', '!Проекты РЦРП', 'Блок №3', f'{datetime.datetime.today().year} год', 'Отчеты')
            if self.args.dont_save_ap:
                self.report_file = Path(self.dir_name, f'{datetime.date.today().strftime("%Y%m%d")} Отчет по выполнению мероприятий КФ [{save_begin_date} - {save_end_date}].xlsx')
            else:
                self.report_file = Path(self.dir_name, f'{datetime.date.today().strftime("%Y%m%d")} Отчет по выполнению мероприятий КФ [{save_begin_date} - {save_end_date}] [АП].xlsx')
        else:
            if os.access(PurePath(self.args.report_file).parents[0], os.W_OK):
                self.report_file = Path(self.args.report_file)
                self.dir_name = PurePath(self.args.report_file).parents[0]
            else:
                print(f'{Colors.RED}Не могу записать файл отчета {self.args.report_file}{Colors.END}')
                sys.exit(140)

        self.end_of_the_year = datetime.datetime(year=self.end_date.year, month=12, day=31, hour=23, minute=59, second=59, microsecond=99999)
        self.sheets = ['Массив', 'mdp_upload_date']
        self.not_done_file = Path(self.dir_name, 'Риски ВОЛС.xlsx')
        self.region_obligations_file = self.not_done_file = Path(self.dir_name, 'Обязательства регионов.xlsx')
        self.upload_date: pd.DataFrame = None

    @staticmethod
    def last_day_of_month(_date: datetime) -> datetime:
        if _date.month == 12:
            return _date.replace(day=31)
        return _date.replace(month=_date.month + 1, day=1) - datetime.timedelta(days=1)

    def get_data(self) -> dict[str, pd.DataFrame]:
        try:
            data_update_age = datetime.datetime.now() - datetime.datetime.fromtimestamp(os.stat(self.url).st_mtime)
            if data_update_age > datetime.timedelta(hours=3):
                if input(f'{Colors.RED}Файл {self.url} обновлялся {(data_update_age.days*24 + data_update_age.seconds/3600):.2f} часов назад! Хотите продолжить обработку данных (y/N)?{Colors.END}').lower() != 'y':
                    sys.exit(12)
            print(f'Получение данных из файла {Colors.GREEN}"{self.url}"{Colors.END}')
            with open(self.url, 'rb') as f:
                g = io.BytesIO(f.read())
            _df = pd.read_excel(g.getvalue(), sheet_name=self.sheets)
            g.close()
            return _df
        except FileNotFoundError as ex:
            raise MyLoggingException(f'Файл {self.url} не существует. Ошибка {ex}')
        except Exception as ex:
            raise MyLoggingException(f'Ошибка при получении данных: {ex}')


    @staticmethod
    def make_date_mask(_df: pd.DataFrame, column_name: str, _begin_date: datetime, _end_date: datetime) -> pd.Series:
        _result = ((_df[column_name] >= _begin_date) & (_df[column_name] <= _end_date))
        return _result

    def make_report(self, _df: pd.DataFrame) -> pd.DataFrame:
        delta_char = f'{chr(0x0394)}'
        _df[['PROGNOZ_DATE', 'PLAN_DATE_END']] = _df[['PROGNOZ_DATE', 'PLAN_DATE_END']].apply(pd.to_datetime)

        rename_columns = {
            'RO': 'Регион',
            'PROGNOZ_DATE': 'Прогноз',
            'CHECK_FACT': 'Факт',
            'PLAN_DATE_END': 'MDP План',
            'RO_CLUSTER': 'Кластер',
            'VIDACHA': 'Выдача',
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

        _df = pd.merge(df_plan, pd.merge(df_prognoz, pd.merge(df_vidacha, df_fact, how='outer', sort=True), how='outer', sort=True), how='outer', sort=True).fillna(value=0).sort_values(by='RO_CLUSTER').rename(columns=rename_columns)

        _df[delta_char] = _df['Факт'] - _df['Прогноз']
        _df.loc["total"] = _df.sum(numeric_only=True)
        _df.at["total", 'Регион'] = "ИТОГО:"
        return _df

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

        if self.upload_date is not None:
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
        mask_bs_pico = df_kpi['BP_ESUP'] == 'Pico Cell_Включение'
        mask_bs_dem = df_kpi['BP_ESUP'] == 'Демонтаж БС/АМС'
        mask_energo = df_kpi['BP_ESUP'] == 'Модернизация энергоснабжения'
        mask_climate = df_kpi['BP_ESUP'] == 'Модернизация климатического оборудования'

        if self.process_year.__len__() == 2:
            mask_plan_year = (df_kpi['PLAN_YEAR'] == self.process_year[0]) | (df_kpi['PLAN_YEAR'] == self.process_year[1])
        else:
            mask_plan_year = df_kpi['PLAN_YEAR'] == self.process_year[0]
        mask_new_bs = df_kpi['CHECK_NEW_PLAN'] == 'Новая'
        mask_check_plan = df_kpi['CHECK_PLAN'] == 'Да'

        df_all_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_pico | mask_bs_dem) & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Всего БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_all_bs), 'Всего БС', report_sheets['Всего БС'])

        df_new_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_pico | mask_bs_dem) & mask_new_bs & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Новые БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_new_bs), 'Новые БС', report_sheets['Новые БС'])

        df_exist_bs = df_kpi[mask_check_plan & (mask_bs_build | mask_bs_rec | mask_bs_pico | mask_bs_dem) & ~mask_new_bs & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Существующие БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_exist_bs), 'Существующие БС', report_sheets['Существующие БС'])

        df_rrl = df_kpi[mask_check_plan & (mask_rrl_build | mask_rrl_rec) & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"РРЛ"{Colors.END}')
        wb.excel_format_table(self.make_report(df_rrl), 'РРЛ', report_sheets['РРЛ'])

        df_energy = df_kpi[mask_check_plan & mask_energo & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Энерго"{Colors.END}')
        wb.excel_format_table(self.make_report(df_energy), 'Энерго', report_sheets['Энерго'])

        df_climate = df_kpi[mask_check_plan & mask_climate & mask_plan_year][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Климатика"{Colors.END}')
        wb.excel_format_table(self.make_report(df_climate), 'Климатика', report_sheets['Климатика'])

        if not self.args.dont_save_ap:
            # Сохраняем АП
            for sheet_name, df_name in [["АП БС", df_all_bs], ["АП РРЛ", df_rrl], ["АП Энерго", df_energy], ["АП Климатика", df_climate]]:
                mask_prognoz_date = self.make_date_mask(df_name, 'PROGNOZ_DATE', self.begin_date, self.end_date)
                print(f'Создаем лист отчета: {Colors.GREEN}"{sheet_name}"{Colors.END}')
                _df = df_name[mask_prognoz_date].sort_values(by=['build_priority', 'RO']).rename(columns=rename_columns)
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
