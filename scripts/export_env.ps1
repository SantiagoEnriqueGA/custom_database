# Export the conda environment named 'segadb_env' to environment.yml
conda env export --no-builds -n segadb_env > environment.yml

# Remove the 'prefix' line from the environment.yml file
(Get-Content environment.yml) | Where-Object { $_ -notmatch '^prefix:' } | Set-Content environment.yml