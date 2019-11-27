#!/bin/expect
set timeout 30
spawn ssh -l somebody 127.0.0.1
expect "password:"
send "alialiali\r"
interact
