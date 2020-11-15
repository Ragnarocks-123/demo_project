cls

@echo off
echo ----------------
echo Start script 
echo ----------------
echo 

del data.db

python main.py passports_blacklist_01052020.xlsx transactions_01052020.xlsx
timeout /t 5 /nobreak>nul

python main.py passports_blacklist_02052020.xlsx transactions_02052020.xlsx
timeout /t 5 /nobreak>nul

python main.py passports_blacklist_03052020.xlsx transactions_03052020.xlsx





