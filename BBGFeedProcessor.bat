set Path=C:\Program Files\Support Tools\;C:\Program Files\Windows Resource Kits\Tools\;C:\WINDOWS\system32;C:\WINDOWS;C:\WINDOWS\System32\Wbem;C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\130\Tools\Binn\;C:\Perl\bin;E:\Perl\bin;C:\Program Files\Windows Imaging\;C:\WINDOWS\system32\WindowsPowerShell\v1.0;C:\Program Files\Microsoft SQL Server\100\Tools\Binn\VSShell\Common7\IDE\;C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\130\Tools\Binn\;C:\Program Files\Microsoft SQL Server\100\DTS\Binn\

cd /D E:\Dealgen\Dealgen_java\BBG2DGD\bin

set CLASSPATH=.;E:\Dealgen\DealGen_Java\BBG2DGD\lib\JHCommon45.jar;E:\Dealgen\DealGen_Java\BBG2DGD\lib\ifxjdbc.jar;E:\Dealgen\DealGen_Java\BBG2DGD\lib\xalan.jar;E:\Dealgen\DealGen_Java\BBG2DGD\lib\xerces.jar;C:\Program Files\Microsoft JDBC Driver 8.4 for SQL Server\sqljdbc_8.4\enu\mssql-jdbc-8.4.1.jre8.jar;

C:\Progra~1\Java\jre1.8.0_261\bin\java -Djava.library.path="C:/Program Files/Microsoft JDBC Driver 8.4 for SQL Server/sqljdbc_8.4/enu/auth/x64" -DPLATFORM=WINDOWS -DROOTDIR=/Dealgen/DealGen_Java/BBG2DGD -DBBG2DGDSRC=E:\DEALGEN com.jhancock.dgd.bbgfeed.BBGFeedProcessor  > E:\dealgen\logs\bbgfeed.log 

echo %ERRORLEVEL%

if errorlevel 1 if not errorlevel 2 goto comp
E:\Dealgen\setexitc 1 
Exit

:comp
E:\Dealgen\setexitc 0 
Exit