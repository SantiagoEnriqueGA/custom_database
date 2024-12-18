
# Calculate comment density in Python files, excluding paths containing "__archive"
Get-ChildItem -Recurse -Filter *.py | Where-Object { 
    -not ($_.FullName -like "*__archive*") 
} | ForEach-Object {
    $lines = Get-Content $_.FullName
    $totalLines = $lines.Count
    $commentLines = ($lines | Where-Object { $_ -match '^\s*#' }).Count
    $commentDensity = if ($totalLines -ne 0) { $commentLines / $totalLines } else { 0 }
    
    [PSCustomObject]@{
        FileName       = $_.FullName -replace ".*custom_database", "custom_database"
        TotalLines     = $totalLines
        CommentLines   = $commentLines
        CommentDensity = ("{0:P2}" -f $commentDensity)
    }
} | Sort-Object -Property {[double]($_.CommentDensity -replace '%')} -Descending | Tee-Object -FilePath "scripts/out/comment_density.txt" | Format-Table -AutoSize