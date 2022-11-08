import argparse
import datetime
import locale
import os
import sys
from pathlib import Path

import pandas as pd
from loguru import logger
from pandas import DataFrame, Series

from Colors import Colors
from FormattedWorkbook import FormattedWorkbook
from MyLoggingException import MyLoggingException


class WeeklyReport:
    def __init__(self):
        self.program_name = Path(__file__).name.split('.')[0]
        self.program_version = "0.1.3"
        self.log_level = 'ERROR'

        logger.remove()
        logger.add(sys.stdout, level=self.log_level)

        self.parser = argparse.ArgumentParser(description=f'{self.program_name} v.{self.program_version}')
        self.parser.add_argument("-b", "--begin-date", type=str, help="Дата начала периода анализа формат YYYY-MM-DD")
        self.parser.add_argument("-e", "--end-date", type=str, help="Дата окончания периода анализа формат YYYY-MM-DD")
        self.args = self.parser.parse_args()

        if self.args.begin_date is None:
            self.begin_date = datetime.date(datetime.date.today().year, datetime.date.today().month, 1)
            save_begin_date = self.begin_date.strftime('%Y-%m-%d')
            self.begin_date = self.begin_date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            save_begin_date = self.args.begin_date
            self.begin_date = f'{self.args.begin_date} {datetime.time(hour=0, minute=0, second=0).strftime("%H:%M:%S")}'

        # Дата конца анализа
        if self.args.end_date is None:
            self.end_date = self.last_day_of_month(datetime.date(datetime.date.today().year, datetime.date.today().month, 1)).strftime('%Y-%m-%d')
            save_end_date = self.end_date
            self.end_date = f'{self.end_date} {datetime.time(hour=23, minute=59, second=59).strftime("%H:%M:%S")}'
        else:
            save_end_date = self.args.end_date
            self.end_date = f'{self.args.end_date} {datetime.time(hour=23, minute=59, second=59).strftime("%H:%M:%S")}'

        self.url = f'\\\\megafon.ru\\KVK\\KRN\\Files\\TelegrafFiles\\ОПРС\\!Проекты РЦРП\\Блок №3\\2022 год\\!!!SQL Блок№3!!!  2022.xlsm'
        # self.url = f'\\\\megafon.ru\\KVK\\KRN\\Files\\TelegrafFiles\\ОПРС\\!Проекты РЦРП\\Блок №3\\2022 год\\Архив\\20221101 !!!SQL Блок№3!!!  2022.xlsm'
        # sheets = ['Массив', 'Рефарминг']
        self.sheets = ['Массив']
        self.report_file = f'\\\\megafon.ru\\KVK\\KRN\\Files\\TelegrafFiles\\ОПРС\\!Проекты РЦРП\\Блок №3\\2022 год\\Отчеты\\{datetime.date.today().strftime("%Y%m%d")} Отчет по ' \
                           f'выполнению мероприятий КФ [{save_begin_date} - {save_end_date}].xlsx'

    @staticmethod
    def last_day_of_month(_date: datetime) -> datetime:
        if _date.month == 12:
            return _date.replace(day=31)
        return _date.replace(month=_date.month + 1, day=1) - datetime.timedelta(days=1)

    def get_data(self) -> dict[str, DataFrame]:
        try:
            print(f'Получение данных из файла {Colors.GREEN}"{self.url}"{Colors.END}')
            _df = pd.read_excel(self.url, sheet_name=self.sheets)
            return _df
        except FileNotFoundError as ex:
            raise MyLoggingException(f'Файл {self.url} не существует. Ошибка {ex}')
        except Exception as ex:
            raise MyLoggingException(f'Ошибка при получении данных: {ex}')

    @staticmethod
    def make_date_mask(_df: DataFrame, column_name: str, _begin_date: str, _end_date: str) -> Series:
        return (_df[column_name] >= pd.to_datetime(_begin_date, yearfirst=True)) & (_df[column_name] <= pd.to_datetime(_end_date, yearfirst=True))

    def make_report(self, _df: DataFrame) -> DataFrame:
        delta_char = f'{chr(0x0394)}'
        _df[['PROGNOZ_DATE', 'PLAN_DATE_END']] = _df[['PROGNOZ_DATE', 'PLAN_DATE_END']].apply(pd.to_datetime)

        rename_columns = {
            'RO': 'Регион',
            'PROGNOZ_DATE': 'Прогноз',
            'CHECK_FACT': 'Факт',
            'PLAN_DATE_END': 'MDP План'
        }
        mask_plan_date = self.make_date_mask(_df, 'PLAN_DATE_END', self.begin_date, self.end_date)
        mask_prognoz_date = self.make_date_mask(_df, 'PROGNOZ_DATE', self.begin_date, self.end_date)
        mask_fact_date = self.make_date_mask(_df, 'TRUNC(A.MIN_DATE_FACT)', self.begin_date, self.end_date)
        mask_check_fact = (_df['CHECK_FACT'] == 1)

        df_plan = _df[mask_plan_date].groupby(['RO']).agg({'PLAN_DATE_END': 'count', }).reset_index()
        df_prognoz = _df[mask_prognoz_date].groupby(['RO']).agg({'PROGNOZ_DATE': 'count', }).reset_index()
        df_fact = _df[mask_fact_date & mask_check_fact].groupby(['RO']).agg({'CHECK_FACT': 'count', }).reset_index()
        _df = pd.merge(df_plan, pd.merge(df_prognoz, df_fact, how='outer', sort=True), how='outer', sort=True).fillna(value=0).rename(columns=rename_columns)

        _df[delta_char] = _df['Факт'] - _df['Прогноз']
        _df.loc["total"] = _df.sum(numeric_only=True)
        _df.at["total", 'Регион'] = "ИТОГО:"
        return _df

    def report_kpi(self, df_kpi: DataFrame) -> FormattedWorkbook:
        report_sheets = {
            'Всего БС': 'all_bs_report',
            'Новые БС': 'new_bs_report',
            'РРЛ': 'rrl_report'
        }

        wb = FormattedWorkbook(logging_level=self.log_level)

        report_columns = [
            'ID_ESUP',
            'BP_ESUP',
            'PROGRAM',
            'CHECK_FACT',
            'RO',
            'NAZ',
            'CHECK_NEW_PLAN',
            'PLAN_DATE_END',
            'PROGNOZ_DATE',
            'PROGNOZ_COMMENT',
            'MDP_PAP',
            'TRUNC(A.MIN_DATE_FACT)',
        ]

        mask_rrl_build = df_kpi['BP_ESUP'] == 'Строительство РРЛ'
        mask_rrl_rec = df_kpi['BP_ESUP'] == 'Переоборудование РРЛ'
        mask_bs_build = df_kpi['BP_ESUP'] == 'Строительство БС/АМС'
        mask_bs_rec = df_kpi['BP_ESUP'] == 'Переоборудование БС'
        mask_bs_pico = df_kpi['BP_ESUP'] == 'Pico Cell_Включение'

        mask_new_bs = df_kpi['CHECK_NEW_PLAN'] == 'Новая'

        df_all_bs = df_kpi[mask_bs_build | mask_bs_rec | mask_bs_pico][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Всего БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_all_bs), 'Всего БС', report_sheets['Всего БС'])

        df_new_bs = df_kpi[mask_bs_build & mask_new_bs][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"Новые БС"{Colors.END}')
        wb.excel_format_table(self.make_report(df_new_bs), 'Новые БС', report_sheets['Новые БС'])

        df_rrl = df_kpi[mask_rrl_build | mask_rrl_rec][report_columns]
        print(f'Создаем лист отчета: {Colors.GREEN}"РРЛ"{Colors.END}')
        wb.excel_format_table(self.make_report(df_rrl), 'РРЛ', report_sheets['РРЛ'])

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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    locale.setlocale(locale.LC_ALL, '')

    wr = WeeklyReport()
    print(f'{wr.program_name} v.{wr.program_version}')
    df = wr.get_data()
    work_book = wr.report_kpi(df[wr.sheets[0]])
    wr.save_report(work_book)

