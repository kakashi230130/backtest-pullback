$ErrorActionPreference = 'Stop'

$taskName = 'binance-analyze-30m'
$workdir  = 'C:\Users\Admin\.openclaw\workspace\binance-candles-backend'

# Run analyze as a one-shot command (Task Scheduler will handle the schedule).
# Use cmd.exe so we can set env vars and set working directory.
$cmd  = 'cmd.exe'
$args = '/c "cd /d ' + $workdir + ' && set DB_POOL_LIMIT=3 && npm run -s analyze"'

$action  = New-ScheduledTaskAction -Execute $cmd -Argument $args

# Repeat every 30 minutes all day, every day.
# Task Scheduler requires repetition duration to be bounded, so we use 1 day.
$trigger = New-ScheduledTaskTrigger -Daily -At 00:00 -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration (New-TimeSpan -Days 1)

$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 2) -MultipleInstances IgnoreNew

# Interactive = will run when you're logged in. (No password needed.)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

$task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings -Principal $principal

if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
  Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

Register-ScheduledTask -TaskName $taskName -InputObject $task | Out-Null
Start-ScheduledTask -TaskName $taskName

Write-Host "Created and started task: $taskName"
