@echo off


echo Charged script execution...
set PYTHONPATH=\\luxapplp04\Share\Gruppo_Demand_Planning\02_NPI\12_Report\E2E NPI Monitoring Dashbaord\script
"C:\VirtualEnv\e2e_env\python.exe" -m src.charged.charged

echo Backorder script execution...
set PYTHONPATH=\\luxapplp04\Share\Gruppo_Demand_Planning\02_NPI\12_Report\E2E NPI Monitoring Dashbaord\script
"C:\VirtualEnv\e2e_env\python.exe" -m src.backorder.backorder

echo Otif script execution...
set PYTHONPATH=\\luxapplp04\Share\Gruppo_Demand_Planning\02_NPI\12_Report\E2E NPI Monitoring Dashbaord\script
"C:\VirtualEnv\e2e_env\python.exe" -m src.otif.otif_append

echo Sales script execution...
set PYTHONPATH=\\luxapplp04\Share\Gruppo_Demand_Planning\02_NPI\12_Report\E2E NPI Monitoring Dashbaord\script
"C:\VirtualEnv\e2e_env\python.exe" -m src.sales.sales

echo Stock script execution...
set PYTHONPATH=\\luxapplp04\Share\Gruppo_Demand_Planning\02_NPI\12_Report\E2E NPI Monitoring Dashbaord\script
"C:\VirtualEnv\e2e_env\python.exe" -m src.stock.stock





echo.
echo Tutti gli script sono stati eseguiti con successo. La finestra si chiuderà in 10 secondi...
timeout /t 10 > nul
exit