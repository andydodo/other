#!/bin/expect
set timeout 30
set username [lindex $argv 0]
set ip [lindex $argv 1]
spawn ssh -l ${username} ${ip}
expect {
  "yes/no" { send "yes\r"; exp_continue }
  "password:" { send "somebody\r" }
}
interact
