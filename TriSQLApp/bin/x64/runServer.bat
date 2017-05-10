rmdir /q /s "Debug - 副本/storage"
rmdir /q /s "Debug/storage"
if exist "Debug - 副本/TriSQLApp.exe" rmdir /q /s "Debug - 副本"
md "Debug - 副本"
copy Debug "Debug - 副本"
"Debug - 副本/TriSQLApp.exe" -s