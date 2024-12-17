"Lines per Python file:"
Get-ChildItem -Recurse -Filter *.py | ForEach-Object { 
    $filePath = $_.FullName -replace "C:\\Users\\sg670w\\OneDrive - AT&T Services, Inc\\Documents\\Projects\\", "`t"
    $lineCount = (Get-Content $_.FullName | Measure-Object -Line).Lines
    "{0,-80}: {1,5}" -f $filePath, $lineCount
}
$totalLines = Get-ChildItem -Recurse -Filter *.py | Get-Content | Measure-Object -Line | Select-Object -ExpandProperty Lines
"_______________________________________________________________________________"
"{0,-87}: {1,5}" -f "Total lines", $totalLines