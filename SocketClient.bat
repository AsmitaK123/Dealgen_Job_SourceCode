set Path=C:\Program Files\Support Tools\;C:\Program Files\Windows Resource Kits\Tools\;C:\WINDOWS\system32;C:\WINDOWS;C:\WINDOWS\System32\Wbem;C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\130\Tools\Binn;C:\Perl\bin;E:\Perl\bin;C:\Program Files\Windows Imaging\;C:\WINDOWS\system32\WindowsPowerShell\v1.0;C:\Program Files\Microsoft SQL Server\100\Tools\Binn\VSShell\Common7\IDE\;C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\130\Tools\Binn\;C:\Program Files\Microsoft SQL Server\100\DTS\Binn\

cd /D E:\Dealgen\Dealgen_java\BBG2DGD\bin

set CLASSPATH=.;E:\Dealgen\DealGen_Java\BBG2DGD\lib\JHCommon45.jar;E:\Dealgen\DealGen_Java\BBG2DGD\lib\ifxjdbc.jar;E:\Dealgen\DealGen_Java\BBG2DGD\lib\xalan.jar;E:\Dealgen\DealGen_Java\BBG2DGD\lib\xerces.jar

C:\Progra~1\Java\jre1.8.0_261\bin\java -DPLATFORM=WINDOWS -DROOTDIR=/Dealgen/DealGen_Java/BBG2DGD -DPRICINGNO=7000 -DSITENO=P7000_S9 -DbbgHost=10.194.106.1 -DbbgAlternateHost=10.194.106.1 -DbbgPort=25227 -DbbgAlternatePort=25227 -DbbgServerHost=10.232.22.69 -DbbgServerPort=7520 -DBBG2DGDSRC=\\AZWAPPCACBBG02\Dealgen\BBG2DGDInBound  com.jhancock.dgd.bbgsocket.SocketClient > E:\dealgen\logs\bbgsocket.log

echo %ERRORLEVEL%

if errorlevel 0 if not errorlevel 2 goto comp
E:\Dealgen\setexitc 1 
exit

:comp
E:\Dealgen\setexitc 0 
exit