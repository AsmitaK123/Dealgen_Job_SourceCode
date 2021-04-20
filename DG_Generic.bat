rem @echo off
rem DG_Generic.bat
rem 9/15/2011
rem Generic ESP batch file for running DG perl scripts
cd /d E:\Dealgen
set path=%SystemRoot%\system32;%SystemRoot%;C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn;E:\esp;e:\perl64\bin

if [%1] == [] goto :NoScript
E:\perl64\bin\perl.exe %1 %2 %3 %4 %5 
IF errorlevel 1 GOTO ERR
setexitc 0
exit 
:NoScript
echo No Script Name passed.

:ERR
echo Error 
setexitc 1
exit 1
