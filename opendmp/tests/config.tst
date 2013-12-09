#!/usr/bin/expect -f

#  $Id: ndma.exp,v 1.2 2005/03/29 17:53:04 bfozard Exp $
#
# Copyright (c) 2004 Data Domain, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

#############################
# include config file
#############################
source ./ndma_exp.conf

#############################
# set global vars
#############################
set CASES_RUN           0
set CASES_PASSED        0
set CASES_FAILED        0

#############################
# print overall results
#############################
proc printResults {} {
    global CASES_RUN
    global CASES_PASSED
    global CASES_FAILED

    puts "\n"
    puts "    TEST CASES RUN:  $CASES_RUN"
    puts " TEST CASES PASSED:  $CASES_PASSED"
    puts " TEST CASES FAILED:  $CASES_FAILED"
    puts "\n"
}

#############################
# increment CASES_PASSED
#############################
proc incrPassed {} {
    global CASES_PASSED

    set CASES_PASSED [expr $CASES_PASSED + 1]
}

#############################
# increment CASES_FAILED
#############################
proc incrFailed {} {
    global CASES_FAILED

    set CASES_FAILED [expr $CASES_FAILED + 1]
}

#############################
# increment CASES_RUN
#############################
proc incrRun {} {
    global CASES_RUN

    set CASES_RUN [expr $CASES_RUN + 1]
}

#############################
# wait for dma prompt
#############################
proc waitForDmaPrompt {device} {
    set timeout 1
    expect {
        -i $device
        "ndma> " {
            return 0
        }
        default {
            puts "\nTIMEOUT: waiting for dma prompt\n"
            send -i $device "\r"
            exp_continue
        }
    }
}

#############################
# wait for transfer complete post
#############################
proc waitForTransferComplete {device chnl num_bytes} {
    set timeout 10
    expect {
        -i $device
        -re "chnl $chnl .*SFER_COM.*ered: (\[0-9\]*) error: (\[A-Z_\]*_ERR)" {
            if { $num_bytes != $expect_out(1,string) } {
                puts "\nexpected $num_bytes transferred, got \
                             $expect_out(1,string)"
                incrFailed
                printResults
                disconnectAndExit $device 2
            }
            if { $expect_out(2,string) != "NDMP_NO_ERR" } {
                puts "\nexpected NDMP_NO_ERROR got $expect_out(2,string)"
                incrFailed
                printResults
                disconnectAndExit $device 2
            }
            return 0
        }
        default {
            puts "\nTIMEOUT: waiting for TRANSFER_COMPLETE\n"
            incrFailed
            printResults
            disconnectAndExit $device 2
        }
    }
}

#############################
# disconnect and exit
#############################
proc disconnectAndExit {device status} {
    send -i $device "disc\n"
    waitForDmaPrompt $device
    send -i $device "quit\n"
    exit $status
}

#############################
# attempt to synchronize with
# ndma output (ie. flush the
# expect input buffer)
#############################
proc sync {device} {
    expect -i $device *
    send -i $device "\r"
    waitForDmaPrompt $device
    expect -i $device *
    send -i $device "\r"
    waitForDmaPrompt $device
    send -i $device "\r"
    waitForDmaPrompt $device
}
    
#############################
# get error status from
# previous ndma request
#############################
proc lastStatus {device expected} {
    global STOP_ON_ERROR

    sync $device
    send -i $device "last_err\n"
    expect -i $device -re "(\[A-Z_\]*_ERR)"
    if { $expected != $expect_out(1,string) } {
        incrFailed
        if { $STOP_ON_ERROR == "stop_on_error" } {
            puts "\nSTOP ERROR: expected $expected,\
                          got $expect_out(1,string)\n\n"
            printResults
            disconnectAndExit $device 2
        }
        puts "\nERROR: expected $expected, got\
                          $expect_out(1,string)\n\n"
    } else {
        incrPassed
    }
    waitForDmaPrompt $device
}

#############################
# get new seek offset after
# a seek request was issued
#############################
proc getSeekOffset {device expected} {
    global STOP_ON_ERROR

    expect -i $device -re ".*new offset (\[0-9\]*).*"
    if { $expected != $expect_out(1,string) } {
        incrFailed
        if { $STOP_ON_ERROR == "stop_on_error" } {
            puts "\nSTOP ERROR: expected new offset $expected,\
                          got $expect_out(1,string)\n\n"
            printResults
            disconnectAndExit $device 2
        }
        puts "\nERROR: expected new offset $expected, got\
                          $expect_out(1,string)\n\n"
    }
}

#############################
# get file size after an open
# request was issued
#############################
proc getFileSize {device} {
    expect -i $device -re ".*size: (\[0-9\]*).*"
    return $expect_out(1,string)
}

#############################
# start the dma
#############################
proc startDMA {} {
    spawn ./ndma
    set device $spawn_id
    waitForDmaPrompt $device
    return $device
}

#############################
# usage
#############################
proc usage {} {
    puts "usage: ndma.exp <test_group> stop_on_error|continue \[iter_count\]"
    puts "        available test_groups:"
    puts "           all"
    puts "           connect"
    puts "           config"
    puts "           file"
}

#############################
# connect test
#############################
proc connect {device} {
    global CONNECT_HOST
    global CONNECT_UNK_HOST
    global CONNECT_USER
    global CONNECT_PASSWD
    global CONNECT_BAD_PASSWD

    send -i $device "disc\r"
    waitForDmaPrompt $device

    puts "\n @@@ CONNECT CASE 1:  NO_AUTH, expecting failure\n"
    incrRun
    send -i $device "connect $CONNECT_HOST\r"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NOT_AUTHORIZED_ERR"
    send -i $device "disc\r"

    puts "\n @@@ CONNECT CASE 2:  AUTH_TEXT, expecting failure\n"
    incrRun
    send -i $device "connect $CONNECT_HOST -t $CONNECT_USER $CONNECT_PASSWD\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NOT_AUTHORIZED_ERR"
    send -i $device "disc\n"

    puts "\n @@@ CONNECT CASE 3:  AUTH_MD5 with bad password\n"
    incrRun
    send -i $device "connect $CONNECT_HOST -c $CONNECT_USER \
                            $CONNECT_BAD_PASSWD\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NOT_AUTHORIZED_ERR"
    send -i $device "disc\n"

    puts "\n @@@ CONNECT CASE 4:  AUTH_MD5, expecting success\n"
    incrRun
    send -i $device "connect $CONNECT_HOST -c $CONNECT_USER $CONNECT_PASSWD\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "disc\n"

    puts "\n @@@ CONNECT CASE 5:  server AUTH_MD5, expecting success\n"
    incrRun
    send -i $device "connect $CONNECT_HOST -c $CONNECT_USER $CONNECT_PASSWD\n"
    waitForDmaPrompt $device
    send -i $device "server_auth 0 -c\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"
}

#############################
# config test
#############################
proc config {device} {
    global CONNECT_HOST
    global CONNECT_USER
    global CONNECT_PASSWD
    global EXPECTED_FS_INFO_ERR
    global EXPECTED_BUTYPE_INFO_ERR
    global SET_EXT_CLASS_VERS

    puts "\n @@@ CONFIG CASE 1:  GET_HOST_INFO, expecting success\n"
    incrRun
    send -i $device "host_info 0\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ CONFIG CASE 2:  GET_SERVER_INFO, expecting success\n"
    incrRun
    send -i $device "server_info 0\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ CONFIG CASE 3:  GET_CONNECTION_TYPE, expecting success\n"
    incrRun
    send -i $device "conn_types 0\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ CONFIG CASE 4:  GET_BU_TYPE, expecting \
                               $EXPECTED_BUTYPE_INFO_ERR\n"
    incrRun
    send -i $device "bu_types 0\n"
    waitForDmaPrompt $device
    lastStatus $device $EXPECTED_BUTYPE_INFO_ERR

    puts "\n @@@ CONFIG CASE 5:  GET_FS_INFO, expecting \
                               $EXPECTED_FS_INFO_ERR\n"
    incrRun
    send -i $device "fs_info 0\n"
    waitForDmaPrompt $device
    lastStatus $device $EXPECTED_FS_INFO_ERR

    puts "\n @@@ CONFIG CASE 6:  GET_EXT, expecting success\n"
    incrRun
    send -i $device "get_ext 0\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ CONFIG CASE 7:  SET_EXT, expecting success\n"
    incrRun
    send -i $device "set_ext 0 $SET_EXT_CLASS_VERS\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ CONFIG CASE 8:  SET_EXT, expecting \
                                             NDMP_EXT_DANDN_ILLEGAL_ERR\n"
    incrRun
    send -i $device "set_ext 0 $SET_EXT_CLASS_VERS\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_EXT_DANDN_ILLEGAL_ERR"
}

#############################
# file test
#############################
proc file {device} {
    global CONNECT_HOST
    global CONNECT_USER
    global CONNECT_PASSWD
    global EXISTING_RW_FILE
    global EXISTING_RO_FILE
    global NON_EXISTING_FILE
    global MAX_OPEN_FILES
    global MAX_WRITE_LEN
    global MAX_READ_LEN
    global FILE_PATTERN
    global LIST_DIR
    global XFER_FILE_FROM
    global XFER_FILE_FROM_2
    global XFER_FILE_TO
    global XFER_FILE_TO_2

    # cleanup from any failed previous run
    send -i $device "delete 0 $NON_EXISTING_FILE/foo\n"
    waitForDmaPrompt $device
    send -i $device "rmdir 0 $NON_EXISTING_FILE/foo\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "rmdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_FROM\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_FROM_2\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_TO\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_TO_2\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 1:  FILE_GET_STATE, expecting success\n"
    incrRun
    send -i $device "file_get_state 0\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 2:  FILE_OPEN -rw, expecting success\n"
    incrRun
    send -i $device "open 0 -rw $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 3:  FILE_OPEN -r, expecting success\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RO_FILE\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 4:  FILE_OPEN -c, expecting success\n"
    incrRun
    send -i $device "open 0 -crw $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 5:  FILE_OPEN -r, expecting FILE_NOT_FOUND_ERR\n"
    incrRun
    send -i $device "open 0 -rw $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_FILE_NOT_FOUND_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 6:  FILE_OPEN -w, expecting PERMISSION_ERR\n"
    incrRun
    send -i $device "open 0 -w $EXISTING_RO_FILE\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_PERMISSION_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 7:  FILE_CLOSE, expecting success\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RO_FILE\n"
    waitForDmaPrompt $device
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 8:  FILE_CLOSE, expecting FILE_NOT_OPEN_ERR\n"
    incrRun
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_FILE_NOT_OPEN_ERR"

    puts "\n @@@ FILE CASE 9:  FILE_OPEN -c, expecting EXISTS_ERR\n"
    incrRun
    send -i $device "open 0 -crw $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_FILE_EXISTS_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 10:  MAX_FILES, expecting success\n"
    incrRun
    set num_open 1
    while { $num_open < $MAX_OPEN_FILES } {
        send -i $device "open 0 -r $EXISTING_RO_FILE\n"
        waitForDmaPrompt $device
        set num_open [expr $num_open + 1]
    }   
    send -i $device "open 0 -r $EXISTING_RO_FILE\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 11:  MAX_FILES + 1, expecting MAX_FILES_ERR\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RO_FILE\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_FILE_MAX_FILES_ERR"
    set num_open 0
    while { $num_open < $MAX_OPEN_FILES } {
        send -i $device "close 0 $num_open\n"
        waitForDmaPrompt $device
        set num_open [expr $num_open + 1]
    }   

    puts "\n @@@ FILE CASE 12:  FILE_WRITE, expecting success\n"
    incrRun
    send -i $device "open 0 -w $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "write 0 0 $MAX_WRITE_LEN $FILE_PATTERN\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 13:  FILE_WRITE, expecting PERMISSION_ERR\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "write 0 0 $MAX_WRITE_LEN $FILE_PATTERN\n"
    lastStatus $device "NDMP_PERMISSION_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 14:  FILE_READ, expecting success\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RO_FILE\n"
    waitForDmaPrompt $device
    send -i $device "read 0 0 $MAX_READ_LEN\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 15:  FILE_READ, expecting PERMISSION_ERR\n"
    incrRun
    send -i $device "open 0 -w $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "read 0 0 $MAX_READ_LEN\n"
    lastStatus $device "NDMP_PERMISSION_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 16:  FILE_SEEK -s, expecting success\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "seek 0 0 -s 5000\n"
    getSeekOffset $device 5000
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 17:  FILE_SEEK -c, expecting success\n"
    incrRun
    send -i $device "seek 0 0 -c 5000\n"
    getSeekOffset $device 10000
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 18:  FILE_SEEK -e, expecting success\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RW_FILE\n"
    set size [getFileSize $device]
    waitForDmaPrompt $device
    send -i $device "seek 0 0 -e -100\n"
    getSeekOffset $device [expr $size - 100]
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 19:  FILE_SEEK, expecting FILE_NOT_OPEN_ERR\n"
    incrRun
    send -i $device "seek 0 0 -c 5000\n"
    lastStatus $device "NDMP_FILE_NOT_OPEN_ERR"

    puts "\n @@@ FILE CASE 20:  FILE_TRUNCATE, expecting success\n"
    incrRun
    send -i $device "open 0 -rw $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "truncate 0 0 5000\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 21:  FILE_TRUNCATE, expecting FILE_NOT_OPEN_ERR\n"
    incrRun
    send -i $device "truncate 0 0 5000\n"
    lastStatus $device "NDMP_FILE_NOT_OPEN_ERR"

    puts "\n @@@ FILE CASE 22:  FILE_TRUNCATE, expecting PERMISSION_ERR\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "truncate 0 0 5000\n"
    lastStatus $device "NDMP_PERMISSION_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 23:  FILE_DELETE, expecting success\n"
    incrRun
    send -i $device "open 0 -crw $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $NON_EXISTING_FILE\n"
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 24:  FILE_DELETE, expecting FILE_NOT_FOUND_ERR\n"
    incrRun
    send -i $device "delete 0 $NON_EXISTING_FILE\n"
    lastStatus $device "NDMP_FILE_NOT_FOUND_ERR"

    puts "\n @@@ FILE CASE 25:  MKDIR, expecting success\n"
    incrRun
    send -i $device "mkdir 0 $NON_EXISTING_FILE\n"
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 26:  MKDIR, expecting EXISTS_ERR\n"
    incrRun
    send -i $device "mkdir 0 $NON_EXISTING_FILE\n"
    lastStatus $device "NDMP_FILE_EXISTS_ERR"

    puts "\n @@@ FILE CASE 27:  RMDIR, expecting success\n"
    incrRun
    send -i $device "rmdir 0 $NON_EXISTING_FILE\n"
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 28:  FILE_DELETE, expecting NOT_FILE_ERR\n"
    incrRun
    send -i $device "mkdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $NON_EXISTING_FILE\n"
    lastStatus $device "NDMP_FILE_NOT_FILE_ERR"
    send -i $device "rmdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 29:  RMDIR, expecting NOT_DIR_ERR\n"
    incrRun
    send -i $device "open 0 -crw $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    send -i $device "rmdir 0 $NON_EXISTING_FILE\n"
    lastStatus $device "NDMP_FILE_NOT_DIR_ERR"
    send -i $device "delete 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 30:  RMDIR, expecting NOT_EMPTY_ERR\n"
    incrRun
    send -i $device "mkdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "open 0 -crw $NON_EXISTING_FILE/foo\n"
    waitForDmaPrompt $device
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    send -i $device "rmdir 0 $NON_EXISTING_FILE\n"
    lastStatus $device "NDMP_FILE_NOT_EMPTY_ERR"
    send -i $device "delete 0 $NON_EXISTING_FILE/foo\n"
    waitForDmaPrompt $device
    send -i $device "rmdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 31:  LIST_DIR, expecting success\n"
    incrRun
    send -i $device "ls 0 $LIST_DIR\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 32:  LIST_DIR, expecting NOT_FOUND_ERR\n"
    incrRun
    send -i $device "ls 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_FILE_NOT_FOUND_ERR"

    puts "\n @@@ FILE CASE 33:  LIST_DIR, expecting NOT_DIR_ERR\n"
    incrRun
    send -i $device "ls 0 $EXISTING_RO_FILE\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_FILE_NOT_DIR_ERR"

    puts "\n @@@ FILE CASE 34:  LIST_DIR at offset, expecting success\n"
    incrRun
    send -i $device "ls 0 $LIST_DIR 5000\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 35:  RENAME, expecting success\n"
    incrRun
    send -i $device "rename 0 $EXISTING_RW_FILE $EXISTING_RW_FILE.xxx\n"
    waitForDmaPrompt $device
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "rename 0 $EXISTING_RW_FILE.xxx $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 36:  RENAME dir, expecting success\n"
    incrRun
    send -i $device "mkdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "rename 0 $NON_EXISTING_FILE $NON_EXISTING_FILE.xxx\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "delete 0 $NON_EXISTING_FILE.xxx\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 37:  RENAME existing, expecting success\n"
    incrRun
    send -i $device "mkdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "mkdir 0 $NON_EXISTING_FILE.xxx\n"
    waitForDmaPrompt $device
    send -i $device "rename 0 $NON_EXISTING_FILE $NON_EXISTING_FILE.xxx\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "delete 0 $NON_EXISTING_FILE.xxx\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 38:  RENAME, expecting NOT_FOUND_ERR\n"
    incrRun
    send -i $device "rename 0 $NON_EXISTING_FILE $NON_EXISTING_FILE.xxx\n"
    lastStatus $device "NDMP_FILE_NOT_FOUND_ERR"

    puts "\n @@@ FILE CASE 39:  RENAME, expecting NOT_FILE_ERR\n"
    incrRun
    send -i $device "mkdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "rename 0 $EXISTING_RW_FILE $NON_EXISTING_FILE\n"
    lastStatus $device "NDMP_FILE_NOT_FILE_ERR"
    send -i $device "rmdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 40:  RENAME, expecting NOT_DIR_ERR\n"
    incrRun
    send -i $device "mkdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "rename 0 $NON_EXISTING_FILE $EXISTING_RW_FILE\n"
    lastStatus $device "NDMP_FILE_NOT_DIR_ERR"
    send -i $device "rmdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 41:  RENAME, expecting NOT_EMPTY_ERR\n"
    incrRun
    send -i $device "mkdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "mkdir 0 $NON_EXISTING_FILE/foo\n"
    waitForDmaPrompt $device
    send -i $device "mkdir 0 $NON_EXISTING_FILE.xxx\n"
    waitForDmaPrompt $device
    send -i $device "rename 0 $NON_EXISTING_FILE.xxx $NON_EXISTING_FILE\n"
    lastStatus $device "NDMP_FILE_NOT_EMPTY_ERR"
    send -i $device "rmdir 0 $NON_EXISTING_FILE/foo\n"
    waitForDmaPrompt $device
    send -i $device "rmdir 0 $NON_EXISTING_FILE\n"
    waitForDmaPrompt $device
    send -i $device "rmdir 0 $NON_EXISTING_FILE.xxx\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 42:  WRITE_META, expecting success\n"
    incrRun
    send -i $device "open 0 -rw $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "write_meta 0 0 500 $FILE_PATTERN\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 43:  WRITE_META, expecting PERMISSION_ERR\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "write_meta 0 0 500 $FILE_PATTERN\n"
    lastStatus $device "NDMP_PERMISSION_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 44:  WRITE_META, expecting NOT_OPEN_ERR\n"
    incrRun
    send -i $device "write_meta 0 0 500 $FILE_PATTERN\n"
    lastStatus $device "NDMP_FILE_NOT_OPEN_ERR"

    puts "\n @@@ FILE CASE 45:  READ_META, expecting success\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RO_FILE\n"
    waitForDmaPrompt $device
    send -i $device "read_meta 0 0 500\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 46:  READ_META, expecting PERMISSION_ERR\n"
    incrRun
    send -i $device "open 0 -w $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "read_meta 0 0 500\n"
    lastStatus $device "NDMP_PERMISSION_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 47:  READ_META, expecting NOT_OPEN_ERR\n"
    incrRun
    send -i $device "read_meta 0 0 500\n"
    lastStatus $device "NDMP_FILE_NOT_OPEN_ERR"

    puts "\n @@@ FILE CASE 48:  SEEK_META -s, expecting success\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "seek_meta 0 0 -s 100\n"
    getSeekOffset $device 100
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 49:  SEEK_META -c, expecting success\n"
    incrRun
    send -i $device "seek_meta 0 0 -c 100\n"
    getSeekOffset $device 200
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 50:  SEEK_META -e, expecting success\n"
    incrRun
    send -i $device "open 0 -rw $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "truncate_meta 0 100\n"
    waitForDmaPrompt $device
    send -i $device "seek_meta 0 0 -e -10\n"
    getSeekOffset $device 90
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 51:  SEEK_META, expecting FILE_NOT_OPEN_ERR\n"
    incrRun
    send -i $device "seek_meta 0 0 -c 100\n"
    lastStatus $device "NDMP_FILE_NOT_OPEN_ERR"

    puts "\n @@@ FILE CASE 52:  TRUNCATE_META, expecting success\n"
    incrRun
    send -i $device "open 0 -rw $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "truncate_meta 0 0 100\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 53:  TRUNCATE_META, expecting FILE_NOT_OPEN_ERR\n"
    incrRun
    send -i $device "truncate_meta 0 0 100\n"
    lastStatus $device "NDMP_FILE_NOT_OPEN_ERR"

    puts "\n @@@ FILE CASE 54:  TRUNCATE_META, expecting PERMISSION_ERR\n"
    incrRun
    send -i $device "open 0 -r $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "truncate_meta 0 0 100\n"
    lastStatus $device "NDMP_PERMISSION_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 55:  COPY single IOV, expecting success\n"
    incrRun
    send -i $device "open 0 -crw $XFER_FILE_FROM\n"
    waitForDmaPrompt $device
    send -i $device "write 0 0 50000 $FILE_PATTERN\n"
    waitForDmaPrompt $device
    send -i $device "open 0 -crw $XFER_FILE_TO\n"
    waitForDmaPrompt $device
    send -i $device "copy 0 -s 0,0,50000 -d 1,0,50000\n"
    waitForDmaPrompt $device
    waitForTransferComplete $device 0 50000
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    send -i $device "close 0 1\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_FROM\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_TO\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 56:  COPY multi->single IOV, expecting success\n"
    incrRun
    send -i $device "open 0 -crw $XFER_FILE_FROM\n"
    waitForDmaPrompt $device
    send -i $device "write 0 0 50000 $FILE_PATTERN\n"
    waitForDmaPrompt $device
    send -i $device "open 0 -crw $XFER_FILE_TO\n"
    waitForDmaPrompt $device
    send -i $device "copy 0 -s 0,0,10000 0,10000,10000 -d 1,0,20000\n"
    waitForDmaPrompt $device
    waitForTransferComplete $device 0 20000
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    send -i $device "close 0 1\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_FROM\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_TO\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 57:  COPY single->multi IOV, expecting success\n"
    incrRun
    send -i $device "open 0 -crw $XFER_FILE_FROM\n"
    waitForDmaPrompt $device
    send -i $device "write 0 0 50000 $FILE_PATTERN\n"
    waitForDmaPrompt $device
    send -i $device "open 0 -crw $XFER_FILE_TO\n"
    waitForDmaPrompt $device
    send -i $device "copy 0 -s 0,0,20000 -d 1,0,10000 1,10000,10000\n"
    waitForDmaPrompt $device
    waitForTransferComplete $device 0 20000
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    send -i $device "close 0 1\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_FROM\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_TO\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 58:  COPY multi->multi IOV, expecting success\n"
    incrRun
    send -i $device "open 0 -crw $XFER_FILE_FROM\n"
    waitForDmaPrompt $device
    send -i $device "write 0 0 50000 $FILE_PATTERN\n"
    waitForDmaPrompt $device
    send -i $device "open 0 -crw $XFER_FILE_TO\n"
    waitForDmaPrompt $device
    send -i $device "copy 0 -s 0,0,10000 0,10000,10000 -d 1,0,10000 1,10000,10000\n"
    waitForDmaPrompt $device
    waitForTransferComplete $device 0 20000
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    send -i $device "close 0 1\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_FROM\n"
    waitForDmaPrompt $device
    send -i $device "delete 0 $XFER_FILE_TO\n"
    waitForDmaPrompt $device


    puts "\n @@@ FILE CASE 59:  LISTEN, expecting success\n"
    incrRun
    send -i $device "file_listen 0\n"
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 60:  LISTEN, expecting ILLEGAL_STATE_ERR\n"
    incrRun
    send -i $device "file_listen 0\n"
    lastStatus $device "NDMP_ILLEGAL_STATE_ERR"
    send -i $device "file_abort 0\n"
    waitForDmaPrompt $device
    send -i $device "file_stop 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 61:  CONNECT, expecting success\n"
    incrRun
    send -i $device "file_listen 0\n"
    waitForDmaPrompt $device
    send -i $device "connect $CONNECT_HOST -c $CONNECT_USER $CONNECT_PASSWD\n"
    waitForDmaPrompt $device
    send -i $device "file_connect 1\n"
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 62:  ABORT, expecting success\n"
    incrRun
    send -i $device "file_abort 0\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "file_abort 1\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 63:  CONNECT, expecting ILLEGAL_STATE_ERR\n"
    incrRun
    send -i $device "file_connect 0\n"
    lastStatus $device "NDMP_ILLEGAL_STATE_ERR"

    puts "\n @@@ FILE CASE 64:  STOP, expecting success\n"
    incrRun
    send -i $device "file_stop 0\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "file_stop 1\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 65:  STOP, expecting ILLEGAL_STATE_ERR\n"
    incrRun
    send -i $device "file_stop 0\n"
    lastStatus $device "NDMP_ILLEGAL_STATE_ERR"

    puts "\n @@@ FILE CASE 66:  RECEIVE, expecting ILLEGAL_STATE_ERR\n"
    incrRun
    send -i $device "receive 0 0,0,5000\n"
    lastStatus $device "NDMP_ILLEGAL_STATE_ERR"

    puts "\n @@@ FILE CASE 67:  SEND/RECEIVE, expecting success\n"
    incrRun
    send -i $device "open 1 -crw $XFER_FILE_FROM\n"
    waitForDmaPrompt $device
    send -i $device "write 1 0 50000 $FILE_PATTERN\n"
    waitForDmaPrompt $device
    send -i $device "seek 1 0 -s 0\n"
    waitForDmaPrompt $device
    send -i $device "open 0 -crw $XFER_FILE_TO\n"
    waitForDmaPrompt $device
    send -i $device "file_listen 0\n"
    waitForDmaPrompt $device
    send -i $device "file_connect 1\n"
    waitForDmaPrompt $device
    send -i $device "send 1 0,0,50000\n"
    waitForDmaPrompt $device
    send -i $device "receive 0 0,0,50000\n"
    waitForTransferComplete $device 0 50000
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 68:  SEND/RECEIVE array, expecting success\n"
    incrRun
    send -i $device "seek 0 0 -s 0\n"
    waitForDmaPrompt $device
    send -i $device "seek 1 0 -s 0\n"
    waitForDmaPrompt $device
    send -i $device "open 0 -crw $XFER_FILE_TO_2\n"
    waitForDmaPrompt $device
    send -i $device "send 1 0,0,50000\n"
    waitForDmaPrompt $device
    send -i $device "receive 0 0,0,25000 1,0,25000\n"
    waitForTransferComplete $device 0 50000
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 69:  SEND array/RECEIVE, expecting success\n"
    incrRun
    send -i $device "seek 0 0 -s 0\n"
    waitForDmaPrompt $device
    send -i $device "seek 0 1 -s 0\n"
    waitForDmaPrompt $device
    send -i $device "seek 1 0 -s 0\n"
    waitForDmaPrompt $device
    send -i $device "send 0 0,0,25000 1,0,25000\n"
    waitForDmaPrompt $device
    send -i $device "receive 1 0,0,50000\n"
    waitForTransferComplete $device 1 50000
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device
    send -i $device "close 0 1\n"
    waitForDmaPrompt $device
    send -i $device "close 1 0\n"
    waitForDmaPrompt $device

    puts "\n @@@ FILE CASE 70:  READ/WRITE_STREAM, expecting success\n"
    incrRun
    send -i $device "write_stream 0 10 $FILE_PATTERN\n"
    waitForDmaPrompt $device
    send -i $device "read_stream 1 10\n"
    lastStatus $device "NDMP_NO_ERR"

    puts "\n @@@ FILE CASE 71:  SYNC, expecting success\n"
    incrRun
    send -i $device "open 0 -rw $EXISTING_RW_FILE\n"
    waitForDmaPrompt $device
    send -i $device "sync 0 0\n"
    lastStatus $device "NDMP_NO_ERR"
    send -i $device "close 0 0\n"
    waitForDmaPrompt $device

    # close file_conn on 1st filer instance
    send -i $device "file_abort 0\n"
    waitForDmaPrompt $device
    send -i $device "file_stop 0\n"
    waitForDmaPrompt $device
    
    # release the 2nd file server instance
    send -i $device "disc 1\n"
    waitForDmaPrompt $device
}

#############################
# all tests 
#############################
proc all {device} {
    connect $device
    config $device
    #file $device
}

#############################
# main
#############################
if { $argc != 2 && $argc != 3 } {
    usage
    exp_exit
}

# get args
set which_test      "[lindex $argv 0]"
set STOP_ON_ERROR   "[lindex $argv 1]"
if { $argc == 3 } {
    set num_iters "[lindex $argv 2]"
} else {
    set num_iters 1
}

# start ndma
set DEVICE [startDMA]

# connect for tests
send -i $DEVICE "connect $CONNECT_HOST -c $CONNECT_USER $CONNECT_PASSWD\n"
waitForDmaPrompt $DEVICE

# run the test
set this_iter 0
while { $this_iter < $num_iters } {
    $which_test $DEVICE
    printResults
    set this_iter [expr $this_iter + 1]
    puts "Completed iteration $this_iter\n"
}

# disconnect and exit
disconnectAndExit $DEVICE 0
