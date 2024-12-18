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

# Generate documentation for each Python file
$files | ForEach-Object {
    Write-Host "Generating documentation for: $_"
    pydoc -w $_
} | Out-File -FilePath "scripts/documentation.txt" -Encoding utf8

# Move all generated .html files to the docs/ folder
$generatedDocs = Get-ChildItem -Recurse -Filter *.html
$destinationFolder = "docs/"
if (-not (Test-Path -Path $destinationFolder)) {
    New-Item -ItemType Directory -Path $destinationFolder
}
$generatedDocs | ForEach-Object {
    Move-Item -Path $_.FullName -Destination $destinationFolder
}

# Add how to run the documentation the scripts/documentation.txt
Add-Content -Path "scripts/documentation.txt" -Value " "
Add-Content -Path "scripts/documentation.txt" -Value "To view the documentation, run the following command in the terminal:"
Add-Content -Path "scripts/documentation.txt" -Value "python -m pydoc -p 8080"
Add-Content -Path "scripts/documentation.txt" -Value "Then open a web browser and navigate to http://localhost:8080/segadb.html"