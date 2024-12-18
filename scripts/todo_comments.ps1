# Find TODO comments in Python files, excluding paths containing "__archive"
Get-ChildItem -Recurse -Filter *.py | Where-Object { 
    -not ($_.FullName -like "*__archive*") 
} | ForEach-Object {
    $file = $_
    $lines = Get-Content $file.FullName
    $lines | ForEach-Object -Begin { $global:lineNumber = 0 } -Process {
        $lineNumber++
        if ($_ -match 'TODO') {
            $todoComment = [PSCustomObject]@{
                FileName   = $file.FullName -replace ".*custom_database", "custom_database"
                LineNumber = $global:lineNumber
                Line       = $_
            }
            $todoComment
        }
    }
} | Tee-Object -FilePath "scripts/out/todo_comments.txt" | Format-Table -AutoSize