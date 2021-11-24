# cucm-zoom-directions-fixer
Zoom recording system is receiving insufficient data from Cisco, script can fix this using cdr logs.

### Why:
  CUCM is sending not enough data into Zoom,
  especially when some third-party system like Genesys is calling through
  their routing points, so Outbound campaign for some agent
  will be like an Inbound call from recorders point of view,
  but in general such call is an Outbound call. Script will fix it.
  In some cases there is lack of direction data for a call.
  In some cases there is a need to put real duration of a call, because
  in case of Cisco IP Communicator this duration value shows real duration,
  a duration after Early Media. CUCM is not sending invites to Zoom
  to record Early Media in case of using Cisco IP Communicator, and
  in this case duration = 0 (call was not established, was early media only).
  
### Note:
  Pls review my parse_cdr_log function, especially
  the first build condition of one_cdr_record_tuple.
  If you are not using Genesys, it may not work correctly.

  You can leave default Synchro delay, just to check how
  script will affect call records on recorders, so in this case
  they will be already synchronized and records in a database
  of recorders should not be of particular value.
  
### Setup python
Python 3 with modules:\
  `yum install python36 python36-psycopg2 python36-xmltodict`\
  Developed and tested using Python 3.6.8 for ZQM 6.3 (Release Date: 30/05/2018)
  should work on all ZQM 6.x versions

### Setup logs syncing
  1) Script should be able to read main Replay config: `/etc/callrec/core.xml`
  2) Configure CUCM to write cdr files in `/opt/callrec/logs/cdr`\
        Make folder and ftp in Replay first: `mkdir -p /opt/callrec/logs/cdr`\
        Then setup in CUCM ftp/sftp for CDR logs writing\
        Subscriber -> ccmservice -> cdrconfiguration -> Billing Application Server Parameters
    For example you can also sync them by cron from other ftp:\
        `* * * * * wget -r -nd -nc ftp://user:pass@ftp/cdr -P /opt/callrec/logs/cdr`\
        `*/10 * * * * find /opt/callrec/logs/cdr -type f -mmin +60 -delete`
  3) Delay in Replay synchro service should be configured in:
        Setting -> Maintenance -> Synchro -> Custom interval period:\
        `floatend=30 minutes days=3`\
        For each Enabled Recorder!
        Note: in my environment I saw 5-15 mins delay in UCM CDR recordings,
        so 30-40 mins delay should be fine to be able to sync modified records.
        Most of delay time will be created by func modify_relevant_records(),
        it will be searching phase, in first cur.execute sql statement.
        You can rewrite further INSERT statements to use copy in psycopg2,
        but I don't think it will allow to update records faster in general.
  4) Script will mark modified records in couples.description
        by writing down IP addresses of the recorders.
        If contact center is using that field then you should find
        some other field to mark records.
