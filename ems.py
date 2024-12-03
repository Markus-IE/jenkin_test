# Project description: This Script is for Emission Monitoring Server that will send email alert when data source have exceeded of parameter.
# Project owner: Markus I. Evangelista
# Revision history:
# 9/26/2024, Markus I. Evangelista, Initial Version.
# 10/2/2024, Markus I. Evangelista, Added rolling average insert, revised limit emailing.

import configparser
import logging
import os
import re
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pyodbc


def query_executor(server_details, query, values=None):
    try:
        with pyodbc.connect(f"DRIVER={server_details.get('driver')};"
                            f"SERVER={server_details.get('server')};"
                            f"DATABASE={server_details.get('database')};"
                            f"UID={server_details.get('username')};"
                            f"PWD={server_details.get('password')}",
                            autocommit=True) as conn:
            with conn.cursor() as cursor:
                if values:
                    cursor.executemany(query, values)
                else:
                    cursor.execute(query)
                    select_pattern = re.compile(r'^\s*SELECT', re.IGNORECASE)
                    if select_pattern.match(query):
                        return cursor.fetchall()
    except Exception as e:
        log(server_details, 2, 0, 1)
        logging.error(f"Query Executor: {e}")


def load_html_template(logging_server, job_id, file_path):
    try:
        with open(file_path, 'r') as file:
            log(logging_server, 1, job_id, 2)
            return file.read()
    except Exception as e:
        log(logging_server, 2, job_id, 3)
        logging.error(f"Load HTML Template: {e}")


def send_email(logging_server, job_id, context, email, regional=None):
    if not regional:
        try:
            html_template = load_html_template(logging_server, job_id, os.path.join(os.getcwd(), "ems_exceedance.html"))
            html_content = html_template.replace('{{logo}}', 'cid:logo_image')
            html_content = html_content.replace('{{stack}}', context.get('stack'))
            html_content = html_content.replace('{{body}}', email.get('body'))
            html_content = html_content.replace('{{data}}', context.get('data'))

            message = MIMEMultipart()
            message['From'] = email.get('email_address')

            to_recipients = email.get('designated_email').split(',')
            cc_recipients = email.get('cc', '').split(',') if email.get('cc') else []
            bcc_recipients = email.get('bcc', '').split(',') if email.get('bcc') else []
            message['To'] = ', '.join(to_recipients)
            message['Cc'] = ', '.join(cc_recipients)
            message['Bcc'] = ', '.join(bcc_recipients)
            all_recipients = to_recipients + cc_recipients + bcc_recipients

            message['Subject'] = email.get('subject')

            message.attach(MIMEText(html_content, "html"))

            with open(os.path.join(os.getcwd(), "logo_ems.png"), 'rb') as logo_file:
                logo = MIMEImage(logo_file.read())
                logo.add_header('Content-ID', '<logo_image>')
                message.attach(logo)

            with smtplib.SMTP(email.get('smtp'), email.get('port')) as session:
                session.starttls()
                session.login(email.get('email_address'), email.get('email_password'))
                session.sendmail(message['From'], all_recipients, message.as_string())

            log(logging_server, 1, job_id, 4)
        except Exception as e:
            log(logging_server, 2, job_id, 5)
            logging.error(f"Exceedance Email : {e}")
    else:
        try:
            html_template = load_html_template(logging_server, job_id, os.path.join(os.getcwd(), "ems_last_transmission.html"))
            html_content = html_template.replace('{{logo}}', 'cid:logo_image')
            html_content = html_content.replace('{{hours}}', str(context.get('last_date_regional').strftime("%A, %d %B, %Y %I:%M %p")))

            message = MIMEMultipart()
            message['From'] = email.get('email_address')

            to_recipients = email.get('designated_email').split(',')
            cc_recipients = email.get('cc', '').split(',') if email.get('cc') else []
            bcc_recipients = email.get('bcc', '').split(',') if email.get('bcc') else []
            message['To'] = ', '.join(to_recipients)
            message['Cc'] = ', '.join(cc_recipients)
            message['Bcc'] = ', '.join(bcc_recipients)
            all_recipients = to_recipients + cc_recipients + bcc_recipients

            message['Subject'] = email.get('subject')

            message.attach(MIMEText(html_content, "html"))

            with open(os.path.join(os.getcwd(), "logo_ems.png"), 'rb') as logo_file:
                logo = MIMEImage(logo_file.read())
                logo.add_header('Content-ID', '<logo_image>')
                message.attach(logo)

            with smtplib.SMTP(email.get('smtp'), email.get('port')) as session:
                session.starttls()
                session.login(email.get('email_address'), email.get('email_password'))
                session.sendmail(message['From'], all_recipients, message.as_string())

            log(logging_server, 1, job_id, 16)
        except Exception as e:
            log(logging_server, 2, job_id, 17)
            logging.error(f"Last Transmission Email: {e}")


def log(server, tag_id, job_id, message_id):
    logging.basicConfig(filename=os.path.dirname(sys.argv[0]) + "\\em.log", level=logging.INFO,
                        format='%(levelname)s - (' + datetime.now().strftime(
                            "%Y/%m/%d %H:%M:%S") + ') - %(message)s')
    try:
        query_executor(server, f"INSERT INTO em_system_log (timestamp, tag_id, job_id, message_id) VALUES ('{datetime.now().strftime('%Y-%m-%d %H:%M')}', {tag_id}, {job_id}, {message_id})")
    except Exception as e:
        logging.error(f'Emission Monitoring Query Executor has encountered error ({str(e)})')


def get_latest_date(server, table, logging_server, job_id=None):
    try:
        if not job_id:
            query = f"SELECT MAX(Date_Time) FROM {table}"
        else:
            query = f"SELECT MAX(Date_Time) FROM {table} WHERE job_id = {job_id}"
        last = query_executor(server, query)
        log(logging_server, 1, job_id, 6)
        return last[0]
    except Exception as e:
        log(logging_server, 2, job_id, 7)
        logging.error(f"Get Latest Date: {e}")


def get_rolling_ave(server, job_id, table, value, date_time, latest_time, logging_server):
    try:
        rolling_data = []
        while latest_time > date_time:
            date_time += timedelta(minutes=5)
            query = f"""
            SELECT
                (SELECT Date_Time
                FROM {table}
                WHERE Date_Time = '{date_time}') AS date_time,
                
                (SELECT AVG({value})
                FROM {table}
                WHERE Date_Time <= '{date_time}'
                AND (Date_Time > DATEADD(hour, -1, '{date_time}'))
                AND ({value} != -9999)) AS rolling_1hour_ave,
        
                (SELECT AVG({value})
                FROM {table}
                WHERE Date_Time <= '{date_time}'
                AND (Date_Time > DATEADD(hour, -3, '{date_time}'))
                AND ({value} != -9999)) AS rolling_3hour_ave,
        
                (SELECT AVG({value})
                FROM {table}
                WHERE Date_Time <= '{date_time}'
                AND (Date_Time > DATEADD(hour, -4, '{date_time}'))
                AND ({value} != -9999)) AS rolling_4hour_ave,
        
                (SELECT TOP(1) {value}
                FROM {table}
                WHERE Date_Time = '{date_time}') AS raw_value
            """
            rolling = query_executor(server, query)
            if rolling[0][0]:
                for row in rolling:
                    rolling_data.append((job_id, row[0].strftime('%Y-%m-%d %H:%M:%S'), row[1], row[2], row[3], row[4]))
        log(logging_server, 1, job_id, 8)
        return rolling_data
    except Exception as e:
        log(logging_server, 2, job_id, 9)
        logging.error(f"Get Rolling Average: {e}")


def build_table(data, unit, pollutant, pollutant_standards, logging_server, job_id):
    try:
        table = ""
        for eachRow in data:
            table += f"""
            <tr>
                <td style='border:1px solid black; border-collapse:collapse; padding: 15px;'>{pollutant}</td>
                <td style='border:1px solid black; border-collapse:collapse; padding: 15px;'>{str(eachRow[0])}</td>
                <td style='border:1px solid black; border-collapse:collapse; padding: 15px;'>{str(round(eachRow[1], 2))} {unit}</td>
                <td style='border:1px solid black; border-collapse:collapse; padding: 15px;'>{pollutant_standards}</td>
            </tr>
            """
        log(logging_server, 1, job_id, 10)
        return table
    except Exception as e:
        log(logging_server, 2, job_id, 11)
        logging.error(f"Build Table: {e}")


def check_regional_transmission(details, latest_envidas, logging_server, email):
    try:
        details['last_date_regional'] = get_latest_date(details, details.get('table_name'), logging_server)[0]
        check_transmission_query = f"""
        SELECT Date_Time
        FROM {details.get('table_name')}
        WHERE Date_Time > '{latest_envidas - timedelta(hours=1)}'
        """
        transmission = query_executor(details, check_transmission_query)
        if not transmission:
            send_email(logging_server, 0, details, email, regional=1)
            log(logging_server, 1, 0, 16)
        else:
            log(logging_server, 1, 0, 18)
    except Exception as e:
        log(logging_server, 2, 0, 17)
        logging.error(f"Regional Transmission: {e}")


class GenerateEM:
        latest_envidas = ''
        last_rolling = ''
    # try:
        object = configparser.RawConfigParser()
        server_properties_path = os.path.join(os.getcwd(), "server.properties")
        object.read(server_properties_path)
        # object.read(f"server.properties")
        general = object["Server"]
        main_server = {
            'driver': 'ODBC Driver 17 for SQL Server',
            'server': general["server"],
            'database': general["database"],
            'username': general["username"],
            'password': general["password"]
        }

        regional_details_query = f"SELECT host_name, user_name, password, database_name, table_name FROM em_regional_details"
        regional_details = query_executor(main_server, regional_details_query)

        job_query = """
        SELECT j.id, host_name, user_name, password, database_name, station_number, timebase, value_number,
        parameter_name, unit_name, pollutant_standards, stack_name, managing_head, designated_email, cc, bcc, name, 
        subject, body, email_address, email_password, smtp, last_execution, port
        FROM em_job j
        INNER JOIN em_server s ON j.server_id = s.id
        INNER JOIN em_station stn ON j.station_id = stn.id
        INNER JOIN em_parameter p ON j.parameter_id = p.id
        INNER JOIN em_stack st ON j.stack_id = st.id
        INNER JOIN em_email_details ed ON j.email_details_id = ed.id
        INNER JOIN em_recipients r ON j.recipients_id = r.id
        where enable = 1
        """
        job = query_executor(main_server, job_query)
        for i, x in enumerate(job):
            last_execution = job[i][22]
            job_id = job[i][0]
            table_name = f"S{job[i][5].zfill(3)}T{job[i][6].zfill(2)}"
            val_name = f"Value{job[i][7]}"
            server = {
                'driver': 'ODBC Driver 17 for SQL Server',
                'server': job[i][1],
                'database': job[i][4],
                'username': job[i][2],
                'password': job[i][3],
            }
            email = {
                'email_address': job[i][19].strip(),
                'designated_email': job[i][13],
                'cc': job[i][14],
                'bcc': job[i][15],
                'subject': job[i][17],
                'body': job[i][18],
                'smtp': job[i][21],
                'email_password': job[i][20].strip(),
                'port': job[i][23]
            }

            latest_envidas = get_latest_date(server, table_name, main_server)[0]  # this is getting a none type error
            last_rolling = get_latest_date(main_server, 'em_rolling_ave', main_server, job_id)[0]

            rolling_data = get_rolling_ave(server, job_id, table_name, val_name, last_rolling, latest_envidas, main_server)
            if rolling_data:
                ins_rolling_data_query = f"INSERT INTO em_rolling_ave (job_id, date_time, rolling_1hour, rolling_3hour, rolling_4hour, raw_value) VALUES (?, ?, ?, ?, ?, ?)"
                query_executor(main_server, ins_rolling_data_query, values=rolling_data)
                log(main_server, 1, job_id, 12)
            else:
                log(main_server, 2, job_id, 13)

            exceed = f"""
            SELECT date_time,
                CASE 
                    WHEN parameter_name = 'SO2' AND rolling_3hour > {job[i][10]} THEN rolling_3hour
                    WHEN parameter_name = 'NO2' AND rolling_3hour > {job[i][10]} THEN rolling_3hour
                    WHEN parameter_name = 'CO' AND rolling_4hour > {job[i][10]} THEN rolling_4hour
                    WHEN parameter_name = 'DUST' AND rolling_1hour > {job[i][10]} THEN rolling_1hour
                    WHEN parameter_name = 'OPACITY' AND raw_value > {job[i][10]} THEN raw_value
                END AS parameter_value
            FROM em_rolling_ave ra
            INNER JOIN em_job j ON ra.job_id = j.id
            FULL JOIN em_parameter p ON j.parameter_id = p.id
            FULL JOIN em_stack s ON j.stack_id = s.id
            WHERE (date_time BETWEEN '{last_rolling + timedelta(minutes=1)}' AND '{latest_envidas}') 
            AND (parameter_name LIKE '%{job[i][8]}%')
            AND (
                (parameter_name = 'SO2' AND rolling_3hour > {job[i][10]}) OR
                (parameter_name = 'NO2' AND rolling_3hour > {job[i][10]}) OR
                (parameter_name = 'CO' AND rolling_4hour > {job[i][10]}) OR
                (parameter_name = 'DUST' AND rolling_1hour > {job[i][10]}) OR
                (parameter_name = 'OPACITY' AND raw_value > {job[i][10]})
            )
            """
            data = query_executor(main_server, exceed)
            if not data:
                update_date_query = f"UPDATE em_job SET last_execution = '{latest_envidas}' WHERE id = {job_id}"
                query_executor(main_server, update_date_query)
                last_execution = latest_envidas
                log(main_server, 1, job_id, 14)
            else:
                context = {
                    'managing_head': job[i][12],
                    'stack': job[i][11],
                    'name': job[i][16],
                    'data': build_table(data, job[i][9], job[i][8], job[i][10], main_server, job_id)
                }
                send_email(main_server, job_id, context, email)

                update_date_query = f"UPDATE em_job SET last_execution = '{latest_envidas}' WHERE id = {job[i][0]}"
                query_executor(main_server, update_date_query)
                log(main_server, 1, job_id, 15)

            for r, d in enumerate(regional_details):
                details = {
                    'driver': 'ODBC Driver 17 for SQL Server',
                    'server': regional_details[r][0],
                    'username': regional_details[r][1],
                    'password': regional_details[r][2],
                    'database': regional_details[r][3],
                    'table_name': regional_details[r][4],
                    'last_date_regional': None
                }
                check_regional_transmission(details, latest_envidas, main_server, email)

    # except TypeError:
    #     logging.error(f"NoneType Check Last Envidas: {latest_envidas}")
    #     logging.error(f"NoneType Check Last Rolling: {last_rolling}")
    # except Exception as e:
    #     # print("Error in executing Emission Monitoring: ", e)
    #     logging.error(f"Main: {e}")
