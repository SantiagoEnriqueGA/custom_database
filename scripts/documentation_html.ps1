# Delete all existing .html files in the docs/ folder
Remove-Item -Path "docs/*.html" -Force

# Generate documentation for Python files in the segadb/ folder, excluding paths containing "__archive"
$files = Get-ChildItem -Recurse -Path "segadb/" -Filter *.py | Where-Object { 
    -not ($_.FullName -like "*__archive*") 
} | ForEach-Object {
    $_.BaseName
}

# Add segadb. to names of files to generate documentation for
$files = $files | ForEach-Object {
    "segadb.$_"
}

# Add the main segadb module to the list of files to document
$files += "segadb"

# Generate documentation for each Python file
$files | ForEach-Object {
    Write-Host "Generating documentation for: $_"
    pydoc -w $_
} | Out-File -FilePath "scripts/out/documentation.txt" -Encoding utf8

# Add custom CSS to each HTML file
$cssContent = @"
<style>
body { background-color: #f0f0f8; }
table.heading tr { background-color: #7799ee; }
.decor { color: #ffffff; }
.title-decor { background-color: #ffc8d8; color: #000000; }
.pkg-content-decor { background-color: #aa55cc; }
.index-decor { background-color: #ee77aa; }
.functions-decor { background-color: #eeaa77; }
.data-decor { background-color: #55aa55; }
.author-decor { background-color: #7799ee; }
.credits-decor { background-color: #7799ee; }
.error-decor { background-color: #bb0000; }
.grey { color: #909090; }
.white { color: #ffffff; }
.repr { color: #c040c0; }
table.heading tr td.title, table.heading tr td.extra { vertical-align: bottom; }
table.heading tr td.extra { text-align: right; }
.heading-text { font-family: helvetica, arial; }
.bigsection { font-size: larger; }
.title { font-size: x-large; }
.code { font-family: monospace; }
table { width: 100%; border-spacing: 0; border-collapse: collapse; border: 0; }
td { padding: 2; }
td.section-title, td.multicolumn { vertical-align: bottom; }
td.multicolumn { width: 25%; }
td.singlecolumn { width: 100%; }
</style>
"@

# Move all generated .html files to the docs/ folder
$generatedDocs = Get-ChildItem -Recurse -Filter *.html
$generatedDocs | ForEach-Object {
    $htmlContent = Get-Content -Path $_.FullName
    $htmlContent = $htmlContent -replace "(<head>)", "`$1`n$cssContent"
    $htmlContent = $htmlContent -replace "<td class=`"extra`">.*?</td>", ""
    Set-Content -Path $_.FullName -Value $htmlContent
}

$destinationFolder = "docs/"
if (-not (Test-Path -Path $destinationFolder)) {
    New-Item -ItemType Directory -Path $destinationFolder
}
$generatedDocs | ForEach-Object {
    Move-Item -Path $_.FullName -Destination $destinationFolder
}

# Add how to run the documentation the scripts/documentation.txt
Add-Content -Path "scripts/out/documentation.txt" -Value " "
Add-Content -Path "scripts/out/documentation.txt" -Value "To view the documentation, run the following command in the terminal:"
Add-Content -Path "scripts/out/documentation.txt" -Value "python -m pydoc -p 8080"
Add-Content -Path "scripts/out/documentation.txt" -Value "Then open a web browser and navigate to http://localhost:8080/segadb.html"