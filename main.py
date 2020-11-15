import os
import subprocess
import pandas as pd
import sqlite3
import xlrd
import sys

conn = sqlite3.connect('data.db')
cursor = conn.cursor()

#########################################
#Процедуры загрузка инфрмации в БД  во временные таблицы из xlsx
def xlsx2sql_1(filePath, con):
	df = pd.read_excel(filePath)
	df.to_sql('DE5_STG_PASSP_BLACK_DT', con=con, if_exists='replace')

if __name__ == '__main__':
	conn = sqlite3.connect('data.db')
	cursor = conn.cursor()

def xlsx2sql_2(filePath, con):
	df = pd.read_excel(filePath)
	df.to_sql('DE5_STG_TRANSACTIONS_DT', con=con, if_exists='replace')

if __name__ == '__main__':
	conn = sqlite3.connect('data.db')
	cursor = conn.cursor()

#########################################
#Создание таблиц и view
def createUserTable():

#Создание таблицы TERMINALS нормализованной структуры 	
	cursor.execute('''	
		CREATE table if not exists  DE5_DWH_FACT_TERMINALS (
			terminal_id varchar(128)  PRIMARY KEY,
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128),
			create_dt datetime default current_timestamp,
			update_dt datetime default NULL,
			deleted_flg integer default 0
		)
		''')

#Создание таблицы CARDS нормализованной структуры 
	cursor.execute('''	
		CREATE table if not exists  DE5_DWH_FACT_CARDS (
			card_num varchar(128) PRIMARY KEY,
			account_num varchar(128),
			create_dt datetime default current_timestamp,
			update_dt datetime default NULL,
			deleted_flg integer default 0,
			FOREIGN KEY (account_num) 
			REFERENCES DE5_DWH_FACT_ACCOUNTS (account_num)
		)
		''')

#Создание таблицы TRANSACTIONS нормализованной структуры 
	cursor.execute('''	
		CREATE table if not exists  DE5_DWH_FACT_TRANSACTIONS (
			trans_id varchar(128) PRIMARY KEY,
			trans_date datetime,
			card_num varchar(128),
			oper_type varchar(128),
			amt int,
			oper_result varchar(128),
			terminal varchar(128),
			create_dt datetime default current_timestamp,
			update_dt datetime default NULL,
			deleted_flg integer default 0,
			FOREIGN KEY (card_num) 
			REFERENCES DE5_DWH_FACT_CARDS (card_num),
			FOREIGN KEY (terminal) 
			REFERENCES DE5_DWH_FACT_TERMINALS (terminal_id)
		)
		''')

#Создание таблицы ACCOUNTS нормализованной структуры 
	cursor.execute('''	
		CREATE table if not exists  DE5_DWH_FACT_ACCOUNTS (
			account_num varchar(128) PRIMARY KEY,
			valid_to DATE,
			client varchar(128),
			create_dt datetime default current_timestamp,
			update_dt datetime default NULL,
			deleted_flg integer default 0,
			FOREIGN KEY (client) 
			REFERENCES DE5_DWH_FACT_CLIENTS (client_id)
		)
		''')

#Создание таблицы CLIENTS нормализованной структуры 
	cursor.execute('''	
		CREATE table if not exists  DE5_DWH_FACT_CLIENTS (
			client_id varchar(128) PRIMARY KEY,
			last_name varchar(128),
			first_name varchar(128),
			patronymic varchar(128),
			date_of_birth DATE,
			passport_num varchar(128),
			passport_valid_to DATE,
			phone varchar(128),
			create_dt datetime default current_timestamp,
			update_dt datetime default NULL,
			deleted_flg integer default 0
		)
		''')

#Создание таблицы витрины отчетности по мошенническим операциям DE5_REP_FRAUD
	cursor.execute('''	
		CREATE table if not exists DE5_REP_FRAUD (
			event_dt datetime,
			passport varchar(128),
			fio varchar(128),
			phone varchar(128),
			event_type varchar(128),
			report_dt datetime
		)
		''')

#Создание таблицы справочника по признакам мошеннических операций DE5_DWH_DIM_REP_FRAUD
	cursor.execute('''	
		CREATE table if not exists DE5_DWH_DIM_REP_FRAUD (
			id integer PRIMARY KEY autoincrement,
			event_type varchar(128)
		)
		''')

#Создание таблицы паспортов «черного списка»
	cursor.execute('''	
		CREATE table if not exists DE5_DWH_FACT_PASSPORT_BLACKLIST (
			passport_num varchar(128),
			entry_dt DATE,
			create_dt datetime default current_timestamp,
			update_dt datetime default NULL
		)
		''')

#Создание временного view DE5_STG_V_DWH_FACT_TERMINALS 
	cursor.execute('''
		CREATE view if not exists DE5_STG_V_DWH_FACT_TERMINALS as 
			SELECT
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				create_dt,
				deleted_flg
			FROM DE5_DWH_FACT_TERMINALS
		''')

#Создание временного view DE5_STG_V_DWH_FACT_CARDS
	cursor.execute('''
		CREATE view if not exists DE5_STG_V_DWH_FACT_CARDS as 
			SELECT
				card_num,
				account_num,
				create_dt,
				deleted_flg
			FROM DE5_DWH_FACT_CARDS

		''')

#Создание временного view DE5_STG_V_DWH_FACT_TRANSACTIONS
	cursor.execute('''
		CREATE view if not exists DE5_STG_V_DWH_FACT_TRANSACTIONS as 
			SELECT
				trans_id,
				trans_date,
				card_num,
				oper_type,
				amt,
				oper_result,
				terminal,
				create_dt,
				deleted_flg
			FROM DE5_DWH_FACT_TRANSACTIONS
		''')

#Создание временного view DE5_STG_V_DWH_FACT_ACCOUNTS
	cursor.execute('''
		CREATE view if not exists DE5_STG_V_DWH_FACT_ACCOUNTS as 
			SELECT
				account_num,
				valid_to,
				client,
				create_dt,
				deleted_flg
			FROM DE5_DWH_FACT_ACCOUNTS
		''')

#Создание временного view DE5_STG_V_DWH_FACT_CLIENTS
	cursor.execute('''
		CREATE view if not exists DE5_STG_V_DWH_FACT_CLIENTS as 
			SELECT 
				client_id,
				last_name,
				first_name,
				patronymic,
				date_of_birth,
				passport_num,
				passport_valid_to,
				phone,
				create_dt,
				deleted_flg
			FROM DE5_DWH_FACT_CLIENTS
		''')

#########################################
#Создание временных таблиц для новых записей
def createNewRows():

	cursor.execute('''
		CREATE table DE5_STG_NEWROWS_DWH_FACT_TERMINALS as 
			SELECT distinct
				t1.terminal,
				t1.terminal_type,
				t1.city,
				t1.address
			FROM DE5_STG_TRANSACTIONS_DT t1
			left join DE5_STG_V_DWH_FACT_TERMINALS t2
			on t1.terminal = t2.terminal_id
			where t2.terminal_id is null
			and t1.date = 
					(
					SELECT max(t3.date) 
					FROM DE5_STG_TRANSACTIONS_DT t3
					where t1.terminal = t3.terminal 
					);
		''')

	cursor.execute('''
		CREATE table DE5_STG_NEWROWS_DWH_FACT_CARDS as 
			SELECT distinct
				t1.card,
				t1.account
			FROM DE5_STG_TRANSACTIONS_DT t1
			left join DE5_STG_V_DWH_FACT_CARDS t2
			on t1.card = t2.card_num
			where t2.card_num is null
			and t1.date = 
					(
					SELECT max(t3.date) 
					FROM DE5_STG_TRANSACTIONS_DT t3
					where t1.card = t3.card 
					);
		''')

	cursor.execute('''
		CREATE table DE5_STG_NEWROWS_DWH_FACT_TRANSACTIONS as 
			SELECT distinct
				t1.trans_id,
				t1.date,
				t1.card,
				t1.oper_type,
				t1.oper_type,
				t1.amount,
				t1.oper_result,
				t1.terminal
			FROM DE5_STG_TRANSACTIONS_DT t1
			left join DE5_STG_V_DWH_FACT_TRANSACTIONS t2
			on t1.trans_id = t2.trans_id
			where t2.trans_id is null
			and t1.date = 
					(
					SELECT max(t3.date) 
					FROM DE5_STG_TRANSACTIONS_DT t3
					where t1.trans_id = t3.trans_id
					);
		''')

	cursor.execute('''
		CREATE table DE5_STG_NEWROWS_DWH_FACT_ACCOUNTS as 
			SELECT distinct
				t1.account,
				t1.account_valid_to,
				t1.client
			FROM DE5_STG_TRANSACTIONS_DT t1
			left join DE5_STG_V_DWH_FACT_ACCOUNTS t2
			on t1.account = t2.account_num
			where t2.account_num is null
			and t1.date = 
					(
					SELECT max(t3.date) 
					FROM DE5_STG_TRANSACTIONS_DT t3
					where t1.account = t3.account 
					);
		''')

	cursor.execute('''
		CREATE table DE5_STG_NEWROWS_DWH_FACT_CLIENTS as 
			SELECT distinct
				t1.client,
				t1.last_name,
				t1.first_name,
				t1.patronymic,
				t1.date_of_birth,
				t1.passport,
				t1.passport_valid_to,
				t1.phone
			FROM DE5_STG_TRANSACTIONS_DT t1
			left join DE5_STG_V_DWH_FACT_CLIENTS t2
			on t1.client = t2.client_id
			where t2.client_id is null 
			and t1.date = 
					(
					SELECT max(t3.date) 
					FROM DE5_STG_TRANSACTIONS_DT t3
					where t1.client = t3.client 
					);
		''')

#########################################
#Создание временных таблиц для обновленных записей
def createUpdateRows():

	cursor.execute('''
		CREATE table DE5_STG_UPDATEROWS_DWH_FACT_TERMINALS as 
			SELECT
				t1.*
			FROM (
				SELECT distinct
					t2.terminal_id,
					t1.terminal,
					t1.terminal_type,
					t1.city,
					t1.address,
					t2.create_dt,
					t2.deleted_flg
				FROM DE5_STG_TRANSACTIONS_DT t1
				inner join DE5_STG_V_DWH_FACT_TERMINALS t2
				on t1.terminal = t2.terminal_id 
				and (
					   t1.terminal_type <> t2.terminal_type
					or t1.city <> t2.terminal_city 
					or t1.address <> t2.terminal_address
					or t2.deleted_flg <> 0
					) 
				and t1.date = 
					(
					SELECT max(t3.date) 
					FROM DE5_STG_TRANSACTIONS_DT t3
					where t1.terminal = t3.terminal 
					)
			) t1
			left join DE5_STG_V_DWH_FACT_TERMINALS  t2
			on t1.terminal_id = t2.terminal_id 
			and t1.terminal_type = t2.terminal_type
			and t1.city = t2.terminal_city
			and t1.address = t2.terminal_address
			and t2.deleted_flg = 0
			where t2.terminal_id is null
		''')

	cursor.execute('''
		CREATE table DE5_STG_UPDATEROWS_DWH_FACT_CARDS  as 
			SELECT
				t1.*
			FROM (
				SELECT distinct
					t2.card_num,
					t1.card,
					t1.account,
					t2.create_dt,
					t2.deleted_flg
				FROM DE5_STG_TRANSACTIONS_DT t1
				inner join DE5_STG_V_DWH_FACT_CARDS t2
				on t1.card = t2.card_num 
				and (
					   t1.account <> t2.account_num
					or t2.deleted_flg <> 0
					)
					and t1.date = 
					(
					SELECT max(t3.date) 
					FROM DE5_STG_TRANSACTIONS_DT t3
					where t1.card = t3.card 
					)
			) t1
			left join DE5_STG_V_DWH_FACT_CARDS  t2
			on t1.card_num = t2.card_num 
			and t1.account = t2.account_num
			and t2.deleted_flg = 0
			where t2.card_num  is null
		''')

	cursor.execute('''
		CREATE table DE5_STG_UPDATEROWS_DWH_FACT_TRANSACTIONS  as 
			SELECT
				t1.*
			FROM (
				SELECT distinct
					t2.trans_id,
					t1.trans_id,
					t1.date,
					t1.card,
					t1.oper_type,
					t1.amount,
					t1.oper_result,
					t1.terminal,
					t2.create_dt,
					t2.deleted_flg
				FROM DE5_STG_TRANSACTIONS_DT t1
				inner join DE5_STG_V_DWH_FACT_TRANSACTIONS t2
				on t1.trans_id = t2.trans_id
				and (
				   t1.date <> t2.trans_date
					or t1.card <> t2.card_num
					or t1.oper_type <> t2.oper_type
					or t1.amount <> t2.amt
					or t1.oper_result <> t2.oper_result
					or t1.terminal <> t2.terminal
					or t2.deleted_flg <> 0
					) 
				and t1.date = 
					(
					SELECT max(t3.date) 
					FROM DE5_STG_TRANSACTIONS_DT t3
					where t1.trans_id = t3.trans_id 
					)
			) t1
			left join DE5_STG_V_DWH_FACT_TRANSACTIONS  t2
			on t1.trans_id = t2.trans_id
			and t1.date = t2.trans_date
			and t1.card = t2.card_num
			and t1.oper_type = t2.oper_type
			and t1.amount = t2.amt
			and t1.oper_result = t2.oper_result
			and t1.terminal = t2.terminal
			and t2.deleted_flg = 0
			where t2.trans_id  is null
		''')

	cursor.execute('''
		CREATE table DE5_STG_UPDATEROWS_DWH_FACT_ACCOUNTS as 
			SELECT
				t1.*
			FROM (
				SELECT distinct
					t2.account_num,
					t1.account,
					t1.account_valid_to,
					t1.client,
					t2.create_dt,
					t2.deleted_flg
				FROM DE5_STG_TRANSACTIONS_DT t1
				inner join DE5_STG_V_DWH_FACT_ACCOUNTS t2
				on t1.account = t2.account_num
				and (
					   t1.account_valid_to <> t2.valid_to
					or t1.client <> t2.client
					or t2.deleted_flg <> 0
					) 
				and t1.date = 
					(
					SELECT max(t3.date) 
					FROM DE5_STG_TRANSACTIONS_DT t3
					where t1.account = t3.account 
					)
			) t1
			left join DE5_STG_V_DWH_FACT_ACCOUNTS  t2
			on t1.account_num = t2.account_num
			and t1.account_valid_to = t2.valid_to
			and t1.client = t2.client
			and t2.deleted_flg = 0
			where t2.account_num is null
		''')

	cursor.execute('''
		CREATE table DE5_STG_UPDATEROWS_DWH_FACT_CLIENTS as 
			SELECT
				t1.*
			FROM (
				SELECT distinct
					t2.client_id,
					t1.client,
					t1.last_name,
					t1.first_name,
					t1.patronymic,
					t1.date_of_birth,
					t1.passport,
					t1.passport_valid_to,
					t1.phone,
					t2.create_dt,
					t2.deleted_flg
				FROM DE5_STG_TRANSACTIONS_DT t1
				inner join DE5_STG_V_DWH_FACT_CLIENTS t2
				on t1.client = t2.client_id 
				and (
					t1.last_name <> t2.last_name 
					or t1.first_name <> t2.first_name
					or t1.patronymic <> t2.patronymic
					or t1.date_of_birth <> t2.date_of_birth
					or t1.passport <> t2.passport_num
					or t1.passport_valid_to <> t2.passport_valid_to
					or t1.phone <> t2.phone
					or t2.deleted_flg <> 0
					) 
				and t1.date = 
					(
					SELECT max(t3.date) 
					FROM DE5_STG_TRANSACTIONS_DT t3
					where t1.client = t3.client 
					)
			) t1
			left join DE5_STG_V_DWH_FACT_CLIENTS  t2
			on t1.client = t2.client_id
			and t1.last_name <> t2.last_name 
			and t1.first_name <> t2.first_name
			and t1.patronymic <> t2.patronymic
			and t1.date_of_birth <> t2.date_of_birth
			and t1.passport <> t2.passport_num
			and t1.passport_valid_to <> t2.passport_valid_to
			and t1.phone <> t2.phone
			and t2.deleted_flg = 0
			where t2.client_id is null
		''')

#########################################
#Создание временных таблиц для обновленных записей
def createDeleteRows():

	cursor.execute('''
		CREATE table DE5_STG_DELETEROWS_DWH_FACT_TERMINALS  as 
			SELECT 
				t1.terminal_id
			FROM DE5_STG_V_DWH_FACT_TERMINALS t1
			left join DE5_STG_TRANSACTIONS_DT t2
			on t1.terminal_id = t2.terminal
			where t2.terminal is null;
		''')

	cursor.execute('''
		CREATE table DE5_STG_DELETEROWS_DWH_FACT_CARDS as 
			SELECT 
				t1.card_num
			FROM DE5_STG_V_DWH_FACT_CARDS t1
			left join DE5_STG_TRANSACTIONS_DT t2
			on t1.card_num = t2.card
			where t2.card is null;
		''')

	cursor.execute('''
		CREATE table DE5_STG_DELETEROWS_DWH_FACT_TRANSACTIONS as 
			SELECT 
				t1.trans_id
			FROM DE5_STG_V_DWH_FACT_TRANSACTIONS t1
			left join DE5_STG_TRANSACTIONS_DT t2
			on t1.trans_id = t2.trans_id
			where t2.trans_id is null;
		''')

	cursor.execute('''
		CREATE table DE5_STG_DELETEROWS_DWH_FACT_ACCOUNTS as 
			SELECT 
				t1.account_num
			FROM DE5_STG_V_DWH_FACT_ACCOUNTS t1
			left join DE5_STG_TRANSACTIONS_DT t2
			on t1.account_num = t2.account
			where t2.account is null;
		''')

	cursor.execute('''
		CREATE table DE5_STG_DELETEROWS_DWH_FACT_CLIENTS as 
			SELECT distinct
				t1.client_id
			FROM DE5_STG_V_DWH_FACT_CLIENTS t1
			left join DE5_STG_TRANSACTIONS_DT t2
			on t1.client_id = t2.client
			where t2.client is null;
		''')

#########################################
#Процедура загрузки данных в таблицы нормализованной структуры
def updateUserTable():

	cursor.execute('''
		UPDATE DE5_DWH_FACT_TERMINALS
		set update_dt  = datetime('now'),  deleted_flg = 1
		where terminal_id in (SELECT terminal_id FROM DE5_STG_DELETEROWS_DWH_FACT_TERMINALS )
		''')

	cursor.execute('''
		INSERT into DE5_DWH_FACT_TERMINALS (
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
			)
		SELECT
			terminal,
			terminal_type,
			city,
			address
		FROM DE5_STG_NEWROWS_DWH_FACT_TERMINALS 
		''')

	cursor.execute('''
		INSERT OR REPLACE INTO DE5_DWH_FACT_TERMINALS (
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			create_dt,
			update_dt,
			deleted_flg
			)
		SELECT
			terminal,
			terminal_type,
			city,
			address,
			create_dt,
			current_timestamp,
			'0'
		FROM DE5_STG_UPDATEROWS_DWH_FACT_TERMINALS 
		''')

	cursor.execute('''
		UPDATE DE5_DWH_FACT_CARDS
		set update_dt  = datetime('now'),  deleted_flg = 1
		where card_num in (SELECT card_num FROM DE5_STG_DELETEROWS_DWH_FACT_CARDS )
		''')

	cursor.execute('''
		INSERT into DE5_DWH_FACT_CARDS (
			card_num,
			account_num
			)
		SELECT
			card,
			account
		FROM DE5_STG_NEWROWS_DWH_FACT_CARDS 
		''')

	cursor.execute('''
		INSERT OR REPLACE INTO DE5_DWH_FACT_CARDS (
			card_num,
			account_num,
			create_dt,
			update_dt,
			deleted_flg
			)
		SELECT
			card,
			account,
			create_dt,
			current_timestamp,
			'0'
		FROM DE5_STG_UPDATEROWS_DWH_FACT_CARDS 
		''')

	cursor.execute('''
		UPDATE DE5_DWH_FACT_TRANSACTIONS
		set update_dt  = datetime('now'),  deleted_flg = 1
		where trans_id in (SELECT trans_id FROM DE5_STG_DELETEROWS_DWH_FACT_TRANSACTIONS )
		''')

	cursor.execute('''
		INSERT into DE5_DWH_FACT_TRANSACTIONS (
			trans_id,
			trans_date,
			card_num,
			oper_type,
			amt,
			oper_result,
			terminal
			)
		SELECT
			trans_id,
			date,
			card,
			oper_type,
			amount,
			oper_result,
			terminal
		FROM DE5_STG_NEWROWS_DWH_FACT_TRANSACTIONS 
		''')

	cursor.execute('''
		INSERT OR REPLACE INTO DE5_DWH_FACT_TRANSACTIONS (
			trans_id,
			trans_date,
			card_num,
			oper_type,
			amt,
			oper_result,
			terminal,
			create_dt,
			update_dt,
			deleted_flg
			)
		SELECT
			trans_id,
			date,
			card,
			oper_type,
			amount,
			oper_result,
			terminal,
			create_dt,
			current_timestamp,
			'0'
		FROM DE5_STG_UPDATEROWS_DWH_FACT_TRANSACTIONS
		''')

	cursor.execute('''
		UPDATE DE5_DWH_FACT_ACCOUNTS
		set update_dt  = datetime('now'),  deleted_flg = 1
		where account_num in (SELECT account_num FROM DE5_STG_DELETEROWS_DWH_FACT_ACCOUNTS )
		''')

	cursor.execute('''
		INSERT into DE5_DWH_FACT_ACCOUNTS (
			account_num,
			valid_to,
			client
			)
		SELECT
			account,
			account_valid_to,
			client
		FROM DE5_STG_NEWROWS_DWH_FACT_ACCOUNTS 
		''')

	cursor.execute('''
		INSERT OR REPLACE INTO DE5_DWH_FACT_ACCOUNTS (
			account_num,
			valid_to,
			client,
			create_dt,
			update_dt,
			deleted_flg
			)
		SELECT
			account,
			account_valid_to,
			client,
			create_dt,
			current_timestamp,
			'0'
		FROM DE5_STG_UPDATEROWS_DWH_FACT_ACCOUNTS
		''')

	cursor.execute('''
		UPDATE DE5_DWH_FACT_CLIENTS
		set update_dt  = datetime('now'),  deleted_flg = 1
		where client_id in (SELECT client_id FROM DE5_STG_DELETEROWS_DWH_FACT_CLIENTS )
		''')

	cursor.execute('''
		INSERT into DE5_DWH_FACT_CLIENTS (
			client_id,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone
			)
		SELECT
			client,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport,
			passport_valid_to,
			phone
		FROM DE5_STG_NEWROWS_DWH_FACT_CLIENTS 
			''')

	cursor.execute('''
		INSERT OR REPLACE INTO DE5_DWH_FACT_CLIENTS (
			client_id,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			create_dt,
			update_dt,
			deleted_flg
			)
		SELECT
			client,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport,
			passport_valid_to,
			phone,
			create_dt,
			current_timestamp,
			'0'
		FROM DE5_STG_UPDATEROWS_DWH_FACT_CLIENTS
		''')

#########################################
	conn.commit()
#########################################

#Процедура загрузки данных в таблицу паспортов «черного списка»
def InsertCentrTable():
	cursor.execute('''
		INSERT into DE5_DWH_FACT_PASSPORT_BLACKLIST (
			passport_num,
			entry_dt,
			create_dt
			)
		SELECT
			passport,
			DATE(start_dt),
			current_timestamp
		FROM DE5_STG_PASSP_BLACK_DT
	''')

#Процедура загрузки данных в таблицу справочник по признакам мошеннических операций
	cursor.execute('''
		INSERT INTO DE5_DWH_DIM_REP_FRAUD (event_type) values	
		('Совершение операции при просроченном или заблокированном паспорте'),
		('Совершение операции при недействующем договоре'),
		('Совершение операций в разных городах в течение одного часа'),
		('Попытка подбора суммы');
    ''')

#########################################
	conn.commit()
#########################################

#Процедуры загрузка инфрмации таблицу витрину отчетности по мошенническим операциям
def createFraud():

	cursor.execute('''
			INSERT INTO DE5_REP_FRAUD (
				event_dt,
				passport,
				fio,
				phone,
				event_type,
				report_dt
				)
			select
				t5.event_dt,
				t6.passport_num,
				t6.last_name||' '||t6.first_name||' '||t6.patronymic as fio,
				t6.phone,
				t7.event_type,
				current_timestamp
			from 
			(SELECT max(f.event_dt) as event_dt, f.client as client, f.event_type as event_type
				FROM
				(
				-- 1 Совершение операции при просроченном или заблокированном паспорте
				SELECT 
				t1.date as event_dt,
				t1.client as client,
				'1' as event_type
				FROM DE5_STG_TRANSACTIONS_DT t1
				where t1.oper_result = 'Успешно' 
				and t1.date > t1.passport_valid_to

				UNION ALL

				--2 Совершение операции при недействующем договоре
				SELECT 
				t1.date as event_dt,
				t1.client as client,
				'2' as event_type
				FROM DE5_STG_TRANSACTIONS_DT t1
				where t1.oper_result = 'Успешно' and
				t1.date > t1.account_valid_to

				UNION ALL

				--3 Совершение операций в разных городах в течение одного часа
				SELECT
				b.to_dttm as event_dt,
				b.client as client,
				'3' as event_type
				FROM
				(
				SELECT a.*,
				cast ((JulianDay(to_dttm) - JulianDay(FROM_dttm)) * 24 * 60 As Integer) as delta
				FROM (
				SELECT 
				date as FROM_dttm,
				lead(date) over(partition by account order by date) as to_dttm,
				client,
				account,
				city,
				lead(city) over (partition by account order by date) as city_2,
        		oper_result,
				lead(oper_result) over (partition by account order by date) as oper_result_2
				FROM DE5_STG_TRANSACTIONS_DT
				where oper_result = 'Успешно'
				) a
				where to_dttm is not null
				and city <> city_2 and oper_result_2 = 'Успешно'
				and delta <= 60) b

				UNION ALL

				--4 Попытка подбора суммы
				SELECT
				d.to_dttm_3 as event_dt,
				d.client as client,
				'4' as event_type
				FROM
				(
				SELECT c.*,
				Cast ((JulianDay(to_dttm_3) - JulianDay(FROM_dttm)) * 24 * 60 As Integer) as delta
				FROM
				(
				SELECT a.*
				FROM
				(
				SELECT 
				date as FROM_dttm,
				lead(date, 1) over(partition by account order by date) as to_dttm_1,
				lead(date, 2) over(partition by account order by date) as to_dttm_2,
				lead(date, 3) over(partition by account order by date) as to_dttm_3,
				client, 
				account,
				oper_result,
				lead(oper_result,1) over (partition by account order by date) as result_1,
				lead(oper_result,2) over (partition by account order by date) as result_2,
				lead(oper_result,3) over (partition by account order by date) as result_3,
				amount,
				lead(amount) over (partition by account order by date) as amount_1,
				lead(amount,2) over (partition by account order by date) as amount_2,
				lead(amount,3) over (partition by account order by date) as amount_3
				FROM DE5_STG_TRANSACTIONS_DT
				) a
				where oper_result='Отказ'
        		and result_1='Отказ' 
       			and result_2='Отказ' 
        		and result_3='Успешно'  
				and amount > amount_1 
       			and amount_1 > amount_2 
        		and amount_2 > amount_3
				) c
				where delta <= 20) d
				) f
				---------------------------
				group by f.client, f.event_type
				---------------------------
				) t5
			inner join DE5_DWH_FACT_CLIENTS t6
			on t5.client = t6.client_id
			inner join DE5_DWH_DIM_REP_FRAUD t7
			on t5.event_type = t7.id
			left join DE5_REP_FRAUD t8
			on t8.event_dt = t5.event_dt 
			and t6.passport_num = t8.passport
			where t8.event_dt is null 
			and t8.passport is null 
			and t8.fio is null
			and t8.phone is null
			order by t5.event_dt
			''')

#########################################
	conn.commit()
#########################################
#Процедура удаления временных таблиц
def clearDB():

	cursor.execute('''	
	drop table if exists DE5_STG_TRANSACTIONS_DT;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_NEWROWS_DWH_FACT_TERMINALS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_NEWROWS_DWH_FACT_CARDS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_NEWROWS_DWH_FACT_TRANSACTIONS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_NEWROWS_DWH_FACT_ACCOUNTS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_NEWROWS_DWH_FACT_CLIENTS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_UPDATEROWS_DWH_FACT_TERMINALS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_UPDATEROWS_DWH_FACT_CARDS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_UPDATEROWS_DWH_FACT_TRANSACTIONS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_UPDATEROWS_DWH_FACT_ACCOUNTS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_UPDATEROWS_DWH_FACT_CLIENTS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_DELETEROWS_DWH_FACT_TERMINALS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_DELETEROWS_DWH_FACT_CARDS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_DELETEROWS_DWH_FACT_TRANSACTIONS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_DELETEROWS_DWH_FACT_ACCOUNTS;
	''')
	cursor.execute('''	
	drop table if exists DE5_STG_DELETEROWS_DWH_FACT_CLIENTS;
	''')
	cursor.execute('''	
	drop table if exists DE5_DWH_DIM_REP_FRAUD;
	''')

#########################################
#Процедура вывода содержимого таблиц
def showTable(tableName):

	cursor.execute(f'SELECT * FROM {tableName}')
	for row in cursor.fetchall():
		print(row)
#########################################
#Выполнение процедур

clearDB()
xlsx2sql_1(sys.argv[1], conn)
xlsx2sql_2(sys.argv[2], conn)
createUserTable()
createNewRows()
createUpdateRows()
createDeleteRows()
updateUserTable()
InsertCentrTable()
createFraud()
#########################################
#Вывод данных из  витрины отчетности  по мошенническим операциям

'''
print('_'*10 + 'UPDATEROWS_TRANSACTIONS')
showTable('DE5_STG_UPDATEROWS_DWH_FACT_TRANSACTIONS')
print('_'*10 + 'UPDATEROWS_CARDS')
showTable('DE5_STG_UPDATEROWS_DWH_FACT_CARDS')
print('_'*10 + 'UPDATEROWS_ACCOUNTS')
showTable('DE5_STG_UPDATEROWS_DWH_FACT_ACCOUNTS')
print('_'*10 + 'UPDATEROWS_TERMINALS')
showTable('DE5_STG_UPDATEROWS_DWH_FACT_TERMINALS')
print('_'*10 + 'UPDATEROWS_CLIENTS')
showTable('DE5_STG_UPDATEROWS_DWH_FACT_CLIENTS')
'''
print('\n' + '#'*55)
print('Таблица витрины  отчетности по мошенническим операциям')
print('#'*55)
showTable('DE5_REP_FRAUD')
