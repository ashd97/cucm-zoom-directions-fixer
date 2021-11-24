#!/usr/bin/env /usr/bin/python3

"""
Developed by Savely (savely.kanevsky@gmail.com)
 03.04.2019 Code is under the MIT License.

For Cisco-Zoom environment ONLY.

"""


import sys
import time
import os
import multiprocessing
from multiprocessing import Process, Manager
from glob import glob
import re
import logging
from logging import handlers

import psycopg2
import xmltodict


def get_all_recorders(source_file: str):
    """
    Args:
      standart path of core.xml

    Returns:
      list of recorders with IP addr and Name

    """


    recorders = []

    with open(source_file) as filed:
        doc = xmltodict.parse(filed.read())
        pool = doc['Configuration']['Database']['Pool']

        for values in range(len(pool)):
            database_string = pool[values]
            if 'Core' in database_string['@name']:
                recorders.append([database_string['Url']['@host'], pool[values]['@name']])

    return recorders



def get_active_phones_of_recorder(recorder: str, dir_of_active_phones):
    """
    Args:
      ip addr of recorder which is comes from core.xml

    Returns:
      list of phone numbers from Outbound and Inbound calls

    """

    conn = psycopg2.connect(dbname='callrec', user='postgres', host=recorder)
    cur = conn.cursor()
    cur.execute("""

      SELECT DN, %s from (
        SELECT
            CALLINGNR as DN
        FROM
            COUPLES
                        WHERE
                        LENGTH(CALLINGNR) = 4
                        AND LENGTH(ORIGINALCALLEDNR) >= 10
                        AND date (START_TS)    >=  date ( NOW() -  INTERVAL '1 DAY' )
        UNION
        SELECT
            ORIGINALCALLEDNR as DN
        FROM
            COUPLES
                        WHERE
                        LENGTH(ORIGINALCALLEDNR) = 4
                        AND LENGTH(CALLINGNR) >= 10
                        AND date (START_TS)    >=  date ( NOW() - INTERVAL '1 DAY' )
      ) as active_phones;

    """, (recorder,))

    result = cur.fetchall()
    cur.close()
    conn.close()

    if result is not None:
        #return result
        dir_of_active_phones.extend(result)
        logging.debug(str(multiprocessing.current_process()) +
                      'get_active_phones_of_recorder() received phones: ' +
                      str(len(result)) + ' for ' + recorder)
    else:
        logging.debug(str(multiprocessing.current_process()) +
                      'get_active_phones_of_recorder() cannot connect to: ' + recorder)



def get_releasing_party(callingpartynumber, finalcalledpartynumber,
                        origcause_value, destcause_value):
    '''
    Function returns string, who initiated the end of a call.

    This should be not a mobile phone number, I assume that
    this is always a length of an internal phone number.
      len(callingpartynumber) < 7
      len(finalcalledpartynumber) < 7

    And this one should a mobile phone number.
      len(callingpartynumber) > 9
      len(finalcalledpartynumber) > 9
    '''

    if (len(callingpartynumber) > 3 and len(callingpartynumber) < 7 and
            origcause_value == '16' and destcause_value == '0' and
            len(finalcalledpartynumber) > 9 and len(finalcalledpartynumber) < 15):
        return 'Agent'
    elif (len(callingpartynumber) > 3 and len(callingpartynumber) < 7 and
          origcause_value == '0' and destcause_value == '16' and
          len(finalcalledpartynumber) > 9 and len(finalcalledpartynumber) < 15):
        return 'Client'
    elif (len(finalcalledpartynumber) > 3 and len(finalcalledpartynumber) < 7 and
          origcause_value == '0' and destcause_value == '16' and
          len(callingpartynumber) > 9 and len(callingpartynumber) < 15):
        return 'Agent'
    elif (len(finalcalledpartynumber) > 3 and len(finalcalledpartynumber) < 7 and
          origcause_value == '16' and destcause_value == '0' and
          len(callingpartynumber) > 9 and len(callingpartynumber) < 15):
        return 'Client'
    else:
        return ''


def get_direction(callingpartynumber, finalcalledpartynumber):
    '''
    General:
      Using length of phone numbers function determines
      whether it is an incoming or outgoing call,
      we assume that internal numbers cannot be longer than mobiles.

    Args:
      Give it two phone numbers - First and last from cdr log.

    Returns:
      list of phone numbers from Outbound and Inbound calls
    '''

    if (len(callingpartynumber) > 3 and len(callingpartynumber) < 7 and
            len(finalcalledpartynumber) > 9 and len(finalcalledpartynumber) < 15):
        return 'OUTGOING'
    elif (len(finalcalledpartynumber) > 3 and len(finalcalledpartynumber) < 7 and
          len(callingpartynumber) > 9 and len(callingpartynumber) < 15):
        return 'INCOMING'
    else:
        return ''



def parse_cdr_log(cdr_log: str, dir_of_active_phones, records_for_uploading):
    '''
    General:
      Its a parser of cdr log itself, from which we are getting less csv data,
      pls we are filtering it by monitored phones, line by line,
      to collect then prepared tuples with needed data.

    Args:
      path to a UCM CDR log file
      directory of phones which were collected on each recorder on monitoring during the day
      list of recorders with IP addr
      records for uploading to some recorder

    Returns:
      Prepared tuples with needed data.

      GLOBALCALLID_CALLID, Calldatetime, duration, origdevicename,
      callingpartynumber,  originalCalledPartyNumber,  lastredirectdn,  finalcalledpartynumber,
      destdevicename,  ORIGCAUSE_VALUE,  DESTCAUSE_VALUE, Generated comment from Cisco docs

    '''

    logging.debug(str(multiprocessing.current_process()) +
                  'thread parse_cdr_log() starts parsing log ' +
                  cdr_log + ' using dir_of_active_phones')

    current_log = open(cdr_log, 'r')

    for line in list(current_log):
        if (not re.search('b00', line) and
                not re.search('zoom_', line) and
                not re.search('INTEGER', line)):
            line = re.sub('"', '', line)
            fields = line.rstrip().split(',')

            # Passing all Inbound and Outbound calls to find relevant records in Zoom
            for phone_recorder_tuple in dir_of_active_phones:
                if (fields[8] in phone_recorder_tuple or fields[30] in phone_recorder_tuple):

                    # fields[8]  = "callingPartyNumber"
                    # fields[29] = "originalCalledPartyNumber"
                    # fields[49] = "lastRedirectDn"
                    # fields[30] = "finalCalledPartyNumber"

                    # In case when Genesys RP is calling (outbound), then direction will be wrong
                    # finalcalledpartynumber and callingpartynumber should be swapped
                    if (len(fields[8]) > 9 and len(fields[8]) < 15 and
                            fields[29] == fields[30] and
                            len(fields[30]) > 3 and len(fields[30]) < 7):
                        one_cdr_record_tuple = (phone_recorder_tuple[1],
                                                fields[2],
                                                time.strftime('%Y-%m-%d %H:%M:%S',
                                                              time.localtime(int(fields[4]))),
                                                fields[55],
                                                fields[30], fields[29], fields[49], fields[8],
                                                fields[57], fields[56], fields[11], fields[33],
                                                get_releasing_party(fields[8], fields[30], fields[11], fields[33]),
                                                get_direction(fields[30], fields[8]))
                        records_for_uploading.append(one_cdr_record_tuple)

                    # Direct calls
                    elif ((len(fields[8]) > 3 and len(fields[8]) < 7 and
                           len(fields[30]) > 9 and len(fields[30]) < 15) or
                          (len(fields[30]) > 3 and len(fields[30]) < 7 and
                           len(fields[8]) > 9 and len(fields[8]) < 15)):
                        one_cdr_record_tuple = (phone_recorder_tuple[1],
                                                fields[2],
                                                time.strftime('%Y-%m-%d %H:%M:%S',
                                                              time.localtime(int(fields[4]))),
                                                fields[55],
                                                fields[8], fields[29], fields[49], fields[30],
                                                fields[56], fields[57], fields[33], fields[11],
                                                get_releasing_party(fields[8], fields[30], fields[33], fields[11]),
                                                get_direction(fields[8], fields[30]))
                        records_for_uploading.append(one_cdr_record_tuple)

    return records_for_uploading


def modify_relevant_records(host_ip, records_for_uploading):
    '''
    General:
      Thread opens connection to the recorder postgresql.
      We need to find relevant couples.id of a call to start inserting
      additional data (cdr_record_tuple) in extdata and fixing direction of a call.
      Connection is closing only after going through
      one group of recordings for this recorder(records_for_uploading)

    Args:
      recorder with IP addr
      prepared records for uploading (parse_cdr_log)

    Returns:
      Its inserting additional data (cdr_record_tuple) to callrec.couple_extdata

    FAQ of cdr_record_tuple:
      cdr_record_tuple[0] = zoom recorder ip
      cdr_record_tuple[1] = GLOBALCALLID_CALLID
      cdr_record_tuple[2] = Calldatetime
      cdr_record_tuple[3] = duration
      cdr_record_tuple[4] = callingpartynumber
      cdr_record_tuple[5] = originalCalledPartyNumber
      cdr_record_tuple[6] = lastredirectdn
      cdr_record_tuple[7] = finalcalledpartynumber
      cdr_record_tuple[8] = origdevicename
      cdr_record_tuple[9] = destdevicename
      cdr_record_tuple[10] = ORIGCAUSE_VALUE
      cdr_record_tuple[11] = DESTCAUSE_VALUE
      cdr_record_tuple[12] = Generated comment from Cisco docs
      cdr_record_tuple[13] = direction conclusion from cdr info
    '''

    logging.debug(str(multiprocessing.current_process()) +
                  ' modify_relevant_records() starts scanning ' +
                  host_ip + ' using records_for_uploading')

    conn = psycopg2.connect(dbname='callrec', user='postgres', host=host_ip)
    cur = conn.cursor()



    for cdr_record_tuple in records_for_uploading:
      # logging.debug('checking cdr_record_tuple =  ' + str(cdr_record_tuple) + '')
        if (str(cdr_record_tuple[0]) == host_ip):
          #logging.debug('thread modify_relevant_records() searching in db ' + host_ip + ' using: ' + cdr_record_tuple[2] + ' ' + cdr_record_tuple[4] + ' ' + cdr_record_tuple[7])

          # Phase of searching relevant records
          # Presumably only this searching phase creates a short but tangible delay
            cur.execute("""
              SELECT
                couples.id
              FROM
                couples
                , couple_extdata
              WHERE
                couples.id = couple_extdata.cplid
                AND couples.description is null -- which means that we are getting not fixed records!
                AND couple_extdata.key = 'JTAPI_CISCO_GLOBAL_CALL_ID'
                AND ( LENGTH(couples.CALLINGNR) + LENGTH(couples.ORIGINALCALLEDNR) ) >= 14
                -- AND couples.start_ts > ( NOW() - INTERVAL '60 minutes' )
                AND %s > ( couples.start_ts - INTERVAL '10 minutes' ) -- calldatetime
                AND %s < ( couples.start_ts + INTERVAL '10 minutes' ) -- calldatetime
                AND %s = couple_extdata.value -- = GLOBALCALLID_CALLID
                AND (
                  ( ( right(couples.callingnr, 10) = right(%s, 10) ) -- callingpartynumber
                  and ( right(couples.originalcallednr, 10) = right(%s, 10) ) ) -- finalcalledpartynumber
                  or
                  ( ( right(couples.callingnr, 10) = right(%s, 10) ) -- finalcalledpartynumber
                  and ( right(couples.originalcallednr, 10) = right(%s, 10) ) ) -- callingpartynumber
                );
            """, (cdr_record_tuple[2],
                  cdr_record_tuple[2],
                  cdr_record_tuple[1],
                  cdr_record_tuple[4],
                  cdr_record_tuple[7],
                  cdr_record_tuple[7],
                  cdr_record_tuple[4]))

            result = cur.fetchone() # 'Segment ID' in callrec interface
            if result is not None:
                couple_id = str(result[0])
                logging.debug(str(multiprocessing.current_process()) +
                              ' modify_relevant_records() searching in db ' +
                              host_ip + ' using: ' + cdr_record_tuple[2] + ' ' +
                              cdr_record_tuple[4] + ' ' + cdr_record_tuple[7] +
                              ' couple id=' + couple_id)

                # https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
                cur.execute("UPDATE couples SET description = %s where couples.id = %s", (host_ip, couple_id))
                cur.execute("UPDATE couples SET callingnr = %s where couples.id = %s", (cdr_record_tuple[4], couple_id))
                cur.execute("UPDATE couples SET originalcallednr = %s where couples.id = %s", (cdr_record_tuple[7], couple_id))
                cur.execute("UPDATE couples SET direction = %s where couples.id = %s", (cdr_record_tuple[13], couple_id))
                cur.execute("INSERT INTO couple_extdata(cplid, key, value) VALUES (%s, %s, %s)", (couple_id, 'UCM_duration', cdr_record_tuple[3]))
                cur.execute("INSERT INTO couple_extdata(cplid, key, value) VALUES (%s, %s, %s)", (couple_id, 'UCM_originalCalledPartyNumber', cdr_record_tuple[5]))
                cur.execute("INSERT INTO couple_extdata(cplid, key, value) VALUES (%s, %s, %s)", (couple_id, 'UCM_lastredirectdn', cdr_record_tuple[6]))
                cur.execute("INSERT INTO couple_extdata(cplid, key, value) VALUES (%s, %s, %s)", (couple_id, 'UCM_origdevicename', cdr_record_tuple[8]))
                cur.execute("INSERT INTO couple_extdata(cplid, key, value) VALUES (%s, %s, %s)", (couple_id, 'UCM_destdevicename', cdr_record_tuple[9]))
                cur.execute("INSERT INTO couple_extdata(cplid, key, value) VALUES (%s, %s, %s)", (couple_id, 'UCM_releasingparty', cdr_record_tuple[12]))
                conn.commit()

                # Perhaps it can be better to use StringIO and copy_from
                # https://www.psycopg.org/docs/cursor.html#cursor.copy_from

    cur.close()
    conn.close()


def scan_all_recorders(recorders, records_for_uploading):

    '''
    Phase 2
    General:
      Each log from CUCM CDR logs will be filtered in parallel mode
      For each recorder of the cluster separate thread will be created
      to scan logs consequentially (modify_relevant_records)

    Args:
      directory of phones which were collected on each recorder on monitoring during the day
      list of recorders with IP addr
      records for uploading to some recorder

    Returns:
      The function is sending logs, by calling parser(modify_relevant_records)
    '''

    # Scan all recorders in parallel to:
    # - to exclude cdr records which were not recorded
    # - to find matching recorder database records for subsequent saturation with CUCM data
    # https://docs.python.org/3.6/library/multiprocessing.html#sharing-state-between-processes

    with Manager() as manager:

        processes = []

        for host_ip in recorders:
            proc = Process(target=modify_relevant_records, args=(str(host_ip[0]), records_for_uploading))
            proc.start()
            processes.append(proc)

        for proc in processes:
            proc.join()



def parse_cdr_dir(cdr_logs_dir: str, cdr_log_preffix: str, dir_of_active_phones, recorders):

    '''
    Phase 1

    General:
      Each log from CUCM CDR logs will be filtered in parallel mode
      Scan(parse_cdr_log) is filtering by previously collected phones(get_dir_of_active_phones)

    Args:
      path of directory with CUCM CDR logs in Replay
      preffix of a CDR log should be standart(cdr_*)
      directory of phones which were collected on each recorder on monitoring during the day
      list of recorders with IP addr

    Returns:
      The function is sending one log file, by calling parser(scan_all_recorders),
      after preparing the required data(parse_cdr_log)
    '''

    with Manager() as manager:
        records_for_uploading = manager.list()  # <-- can be shared between processes.
        processes = []
        cdr_logs = [y for x in os.walk(cdr_logs_dir) for y in glob(os.path.join(x[0], cdr_log_preffix))]

        for cdr_log in cdr_logs:
            proc = Process(target=parse_cdr_log, args=(cdr_log, dir_of_active_phones, records_for_uploading))
            proc.start()
            processes.append(proc)

        for proc in processes:
            proc.join()

        # start Phase 2
        scan_all_recorders(recorders, records_for_uploading)



def get_dir_of_active_phones(main_replay_config):

    '''
    General:
      Scanning each recorder to collect phone directory of the recorder cluster

    Args:
      standart zoom path of core.xml

    Returns:
      list of recorders with IP addr
      phone directory (which phones correspond to specific recorders)

    '''

    # Scan all recorders in parallel to collect phones in dir_of_active_phones
    # https://docs.python.org/3.6/library/multiprocessing.html#sharing-state-between-processes

    with Manager() as manager:
        dir_of_active_phones = manager.list()  # <-- can be shared between processes.
        processes = []
        recorders = get_all_recorders(main_replay_config)

        for host_ip in recorders:
            proc = Process(target=get_active_phones_of_recorder, args=(str(host_ip[0]), dir_of_active_phones))
            proc.start()
            processes.append(proc)

        for proc in processes:
            proc.join()

        dir_of_active_phones.sort()

        # print(dir_of_active_phones, sep = "\n")
        return recorders, list(dir_of_active_phones)



def main():

    '''
    General:
      All paths below are quite standart, them should be correct for any environment.
      Zoom temp folder: /opt/callrec/tmp/
      Replay config: /etc/callrec/core.xml
      preffix: cdr_*

    '''

    working_dir = '/opt/callrec/tmp/'
    cdr_logs_dir = '/opt/callrec/logs/cdr'
    cdr_log_preffix = 'cdr_*'
    logname = 'cdr_importer.log'
    main_replay_config = '/etc/callrec/core.xml'

    # https://docs.python.org/3/howto/logging-cookbook.html
    log = logging.getLogger('')
    log.setLevel(logging.DEBUG)
    myformat = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(myformat)
    log.addHandler(ch)
    fh = handlers.RotatingFileHandler(working_dir + logname, maxBytes=(1048576*5), backupCount=7)
    fh.setFormatter(myformat)
    log.addHandler(fh)


    recorders, dir_of_active_phones = get_dir_of_active_phones(main_replay_config)

    parse_cdr_dir(cdr_logs_dir, cdr_log_preffix, dir_of_active_phones, recorders)


if __name__ == '__main__':
    main()
