import io
import re
import csv
import time
import logging
import requests
import lxml.html
from pathlib import Path
from datetime import datetime, date, timedelta
from database_mysql import Mysql
from settings import config

BASE_DIR = Path(__file__).parent

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO, filename=BASE_DIR / 'stackcommerce.log')

stack_config = config['stackcommerce']


class StackBot:
    """
    class to login and get data from StackCommerce
    """

    def __init__(self):
        logging.info('Initialize bot.')
        self.user = stack_config['user']
        self.password = stack_config['password']
        self.session = requests.Session()

    def __enter__(self):
        return self

    def __exit__(self, *excep):
        logging.info('Sign out from Stack')
        self.session.get(url='https://partners.stackcommerce.com/sign_out')
        logging.info('Close session.')
        self.session.close()

    def log_in(self):
        """
        Метод парсинга токена с начальной страницы
        и последующей авторизации сессии в Stack.
        После авторизации делегирующий токен и данные о партнере записуются в соответствующие атрибуты
            (необходимы для указания в headers всех последующих запросов)
        """
        logging.info('Collect token for login.')
        response = self.session.get('https://partners.stackcommerce.com/sign_in#/sales')
        doc = lxml.html.fromstring(response.text)
        self.auth_token = doc.xpath('//input[@name="authenticity_token"]/@value')[0]
        logging.info('Token has been collected.')
        self.body = {
            'authenticity_token': self.auth_token,
            'commit': 'Log In',
            'user[email]': self.user,
            'user[password]': self.password,
            'user[remember_me]': 0,
        }

        self.response = self.session.post(url='https://partners.stackcommerce.com/session', data=self.body)
        # Проверка успешности логина
        doc = lxml.html.fromstring(self.response.text)
        login_error = doc.xpath('//div[contains(@class, "alert-danger")]')
        if login_error:
            logging.warning('Issue with login')
            logging.info(login_error[0].text)
            return False

        self.response = self.session.get(url='https://partners.stackcommerce.com/#/orders')
        logging.info('Login succesfully.')
        # Парсинг делегирующего токена и данных о партнере
        doc = lxml.html.fromstring(self.response.text)
        app_cntrl_string = doc.xpath('//div[@data-ng-controller="AppCtrl"]/@ng-init')[0]
        delegate_token_pattern = '"delegateToken":"(.+)","authToken"'
        self.delegate_token = re.findall(pattern=delegate_token_pattern, string=app_cntrl_string)[0]
        partner_pattern = '{"vendor":\[(.+)\]}'
        self.partner = re.findall(pattern=partner_pattern, string=app_cntrl_string)[0]
        logging.info('Delegate token and partner\'s data has been scrapped.')

    def orders_batches(self, date_from: date, date_to: date):
        """
        Метод для получения id-шников пакетов заказов
        лучше передавать сюда диапазон в 30 дней (максимум сколько возвращается в ответе от Stack)
        чтобы избежать по пагинации
        """
        date_from = datetime.strftime(date_from, '%Y-%m-%dT22:00:00.000Z')
        date_to = datetime.strftime(date_to, '%Y-%m-%dT22:00:00.000Z')

        params = {
            'end_at': date_to,
            'order_view': 1,
            'start_at': date_from
        }

        headers = {
            'X-Current-Partner': self.partner,
            'X-Stack-Access-Token': self.delegate_token
        }
        self.session.headers.update(headers)
        logging.info('Request orders batches...')
        order_batches_url = 'https://partners.stackcommerce.com/api/vendor/batches'
        response = self.session.get(url=order_batches_url, params=params)

        if response.status_code != 200:
            logging.warning(response.text)
        logging.info(f'Period {date_from} - {date_to}: Batches ids have been collected.')
        # Возвращаем список batch id с конвертацией в строки
        return [str(each['id']) for each in response.json()['batches']]

    def download_orders(self, batches: list):
        """
        TODO - проверить максимально возможное кол-во id-шников в одном запросе
        """
        orders_url = 'https://partners.stackcommerce.com/vendor/batches?cur_partner=vendor-2800'
        batches_string = '&batch_ids%5B%5D=' + '&batch_ids%5B%5D='.join(batches)
        logging.info('Request order report from Stack')
        orders_response = self.session.get(url=orders_url+batches_string)

        if orders_response.status_code != 200:
            logging.warning('Issue with request.')
            logging.info(orders_response.text)

        logging.info(f'Order report received.')
        return orders_response

    def download_earnings(self, date_from: date, date_to: date):
        """
        TODO - проверить, возможно для earnings не нужно передавать делегирующий токен и данные о партнере
        (достаточно только диапазон дат
        [{"key":"cur_partner","value":"vendor-2800","description":""},{"key":"start_at","value":"2019-10-01T00:00:00-07:00","description":""},{"key":"end_at","value":"2019-11-01T23:59:59-07:00","description":""}]
        """
        date_from = datetime.strftime(date_from, '%Y-%m-%dT00:00:00-07:00')
        date_to = datetime.strftime(date_to + timedelta(days=1), '%Y-%m-%dT23:59:59-07:00')

        params = {
            'cur_partner': 'vendor-2800',
            'start_at': date_from,
            'end_at': date_to
        }

        url = 'https://partners.stackcommerce.com/earnings.csv'
        logging.info('Request earnings report from Stack')
        earnings_response = self.session.get(url, params=params)

        if earnings_response.status_code != 200:
            logging.warning('Issue with request.')
            logging.info(earnings_response.text)

        logging.info(f'Period {date_from} - {date_to}:Earning report received.')
        return earnings_response


def update_database(sql_delete_query: str, sql_insert_query: str, data: str):

    with Mysql() as conn:
        conn.autocommit = False
        cursor = conn.cursor()

        # Delete old data
        cursor.execute(sql_delete_query)
        logging.info(f'{cursor.rowcount} rows have been deleted from database')

        # Insert new data
        cursor.execute(sql_insert_query)
        logging.info(f'{cursor.rowcount} rows have been added to database')

        conn.commit()
        cursor.close()


def generate_date_list():
    date_list = []
    date_from = date(2019, 1, 1)
    current_date = date_from
    while True:
        next_date = current_date + timedelta(days=1)

        if next_date.month != current_date.month:
            date_list.append((date_from, current_date))
            date_from = next_date

        if next_date.year == 2020:
            break

        current_date += timedelta(days=1)

    return date_list


def main():
    date_to = date.today()
    date_from_orders = date_to - timedelta(days=30)
    date_from_earnings = date_to.replace(day=1)

    with StackBot() as bot:
        bot.log_in()
        time.sleep(2)
        batches = bot.orders_batches(date_from_orders, date_to)
        time.sleep(2)
        orders_report = bot.download_orders(batches)
        time.sleep(2)
        earnings_report = bot.download_earnings(date_from_earnings, date_to)

        # Orders
        logging.info('Prepare orders data to upload.')
        orders_csv_data = csv.reader(io.StringIO(orders_report.text), delimiter=',')
        data_to_upload = str([tuple(each) for each in list(orders_csv_data)[1:]]).strip('[]')

        sql_delete_query = f"""
            delete from stackcommerce.orders
            where date(order_date) >= '{datetime.strftime(date_from_orders, '%Y-%m-%d')}'
        """

        sql_insert_query = f"""
            insert ignore into stackcommerce.orders
            values {data_to_upload}
        """

        update_database(sql_delete_query, sql_insert_query, data_to_upload)

        # Earnings

        logging.info('Prepare earnings data to upload.')
        earnings_csv_data = csv.reader(io.StringIO(earnings_report.text), delimiter=',')
        data_to_upload = str([tuple([date_from_earnings.year] + [date_from_earnings.month] + each) for each in list(earnings_csv_data)[1:]]).strip('[]')

        sql_delete_query = f"""
            delete from stackcommerce.earnings
            where report_year = {date_from_earnings.year} AND report_month = {date_from_earnings.month}
        """

        sql_insert_query = f"""
            insert ignore into stackcommerce.earnings
            values {data_to_upload}
        """

        update_database(sql_delete_query, sql_insert_query, data_to_upload)


if __name__ == '__main__':
    main()
