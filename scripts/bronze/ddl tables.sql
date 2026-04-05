/*
this code should run before the ingestion, it creates expandium and sharepoint tables and adding meta data for tracking, and ingestion track tables (ingest_files and ingest_runs) that helps on
tracking the runs with it's primary key, time of start and end, status, and how many rows, and tracking files with there primary key, the id of the run that means
which runs they were in, source syteme (like expandium or sharepoint), processed at, row counts and it status
*/

DROP TABLE IF EXISTS bronze.exp_etcs_calls;
CREATE TABLE bronze.exp_etcs_call(
    start_time TIMESTAMP DEFAULT NOW(),
    stop_time VARCHAR(50),
    call_setup_duration VARCHAR(50),
    transaction_duration VARCHAR(50),                    
    etcs_baseline SMALLINT,
    system_version DECIMAL(3,1),
    nid_engine INT NOT NULL,
    nid_operational INT,
    imsi BIGINT,
    msisdn BIGINT,                         
    imei BIGINT,
    calling_number BIGINT,
    called_number INT,
    gsmr_connected VARCHAR(50),
    etcs_connected VARCHAR(50),
    start_nid_c INT,
    start_nid_bg INT,
    stop_nid_c INT,
    stop_nid_bg INT,
    stop_d_lrbg INT,
    root_failure VARCHAR(50),
    end_domain VARCHAR(50),
    protocol_layer VARCHAR(50),
    end_event VARCHAR(255),
    end_cause VARCHAR(255),
    isdn_port_probe VARCHAR(255),
	_batch_id TEXT REFERENCES bronze.ingest_runs(batch_id),  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS bronze.exp_hdlc_frame_errors;
CREATE TABLE bronze.exp_hdlc_frame_errors(
    start_time TIMESTAMP DEFAULT NOW(),
    stop_time TIMESTAMP,
    frame_error_time TIMESTAMP,
    isdn_port_probe VARCHAR(255),
    nid_engine INT,
    nid_operational INT,
    calling_number BIGINT,
    called_number INT,
    last_nid_c INT,
    last_nid_bg INT,
    m_level VARCHAR(20),
    m_mode VARCHAR(50),
    direction VARCHAR(50),
    frame_error VARCHAR(100),
    frame_error_retransmission_count INT,
	_batch_id TEXT REFERENCES bronze.ingest_runs(batch_id),  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS bronze.exp_subscriber_matrix;
CREATE TABLE bronze.exp_subscriber_matrix(
    imsi BIGINT,
    last_imsi_time TIMESTAMP,
    msisdn BIGINT,
    last_msisdn_time TIMESTAMP,
    nid_engine INT,
    last_nid_engine_time TIMESTAMP,
    fn_ct3 BIGINT,
    last_fn_ct3_time TIMESTAMP,
    fn_ct4 BIGINT,
    last_fn_ct4_time TIMESTAMP,
	_batch_id TEXT REFERENCES bronze.ingest_runs(batch_id),  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS bronze.exp_ho_tracing;
CREATE TABLE bronze.exp_ho_tracing(
    start_time TIMESTAMP DEFAULT NOW(),
    stop_time TIMESTAMP,
    ho_start_time TIMESTAMP,
    ho_duration DECIMAL(10,3),
    src_lac INT,
    src_ci INT,
    trg_lac INT,
    trg_ci INT,
    imsi BIGINT,
    msisdn BIGINT,
    imei BIGINT,
    ms_power VARCHAR(50),
    call_type VARCHAR(50),
    ho_type VARCHAR(50),
    ho_end_event VARCHAR(100),
    ho_end_cause VARCHAR(100),
    ho_cause VARCHAR(100),
	_batch_id TEXT REFERENCES bronze.ingest_runs(batch_id),  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS bronze.exp_vgcs_vbs_rec_tracing;
CREATE TABLE bronze.exp_vgcs_vbs_rec_tracing(
    start_time TIMESTAMP DEFAULT NOW(),
    stop_time TIMESTAMP,
    application_type VARCHAR(100),
    transaction_type VARCHAR(50),
    transaction_subtype VARCHAR(50),
    functional_number BIGINT,
    imsi BIGINT,
    tmsi VARCHAR(20),
    msisdn BIGINT,
    gid INT,
    area INT,
    gcr BIGINT,
    priority VARCHAR(20),
    start_lac INT,
    start_ci INT,
    establishment_delay VARCHAR(50),
    sccp_success_rate VARCHAR(20),
    cell_success_rate VARCHAR(20),
    dispatcher_success_rate VARCHAR(20),
    end_user VARCHAR(100),
    vgcs_duration VARCHAR(50),
	_batch_id TEXT REFERENCES bronze.ingest_runs(batch_id),  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS bronze.exp_transaction_tracing;
CREATE TABLE bronze.exp_transaction_tracing(
    start_time TIMESTAMP DEFAULT NOW(),
    stop_time TIMESTAMP,
    call_setup_duration VARCHAR(50),
    establishment_delay VARCHAR(50),
    transaction_duration VARCHAR(50),
    start_lac INT,
    start_ci INT,
    stop_lac INT,
    stop_ci INT,
    nid_engine INT,
    nid_operational INT,
    tmsi VARCHAR(20),
    reallocated_tmsi VARCHAR(20),
    imsi BIGINT,
    msisdn BIGINT,
    imei BIGINT,
    calling_number BIGINT,
    called_number BIGINT,
    dest_route_address VARCHAR(100),
    functional_number VARCHAR(50),
    functional_number_ct VARCHAR(100),
    direction VARCHAR(50),
    transaction_type VARCHAR(50),
    transaction_subtype VARCHAR(50),
    application_type VARCHAR(100),
    gsmr_connected VARCHAR(20),
    priority VARCHAR(20),
    root_failure VARCHAR(50),
    protocol_layer VARCHAR(50),
    end_event VARCHAR(255),
    end_cause VARCHAR(255),
    gb_ciphering_algo VARCHAR(50),
	_batch_id TEXT REFERENCES bronze.ingest_runs(batch_id),  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS bronze.sp_ertms_deconnixions;
CREATE TABLE bronze.sp_ertms_disconnects (
    nombre_ordre INT,
    derniere_7_jours BOOLEAN,
    date DATE,
    heure TIME,
    numero_train INT,
    rame VARCHAR(50),
    motrice_cab VARCHAR(50),
    mrm VARCHAR(50),
    imei BIGINT,
    sens VARCHAR(50),
    km VARCHAR(50),
    intervalle VARCHAR(100),
    niveau_etcs VARCHAR(20),
    evenement VARCHAR(255),
    cause_racine TEXT,
    analyse_smmrgv TEXT,
    sous_systeme_mis_en_cause VARCHAR(255),
    action TEXT,
    execution_ho VARCHAR(255),
    rxqual VARCHAR(50),
    rxlev VARCHAR(50),
    voisinage_10sec VARCHAR(100),
    com_dte_dce VARCHAR(50),
    ecart VARCHAR(50),
    retransmission_trames_hdlc_t70 VARCHAR(100),
    alarmes_bts VARCHAR(255),
    traite_par VARCHAR(100),
    cire VARCHAR(100),
    ciers VARCHAR(100),
    acquittement_auc VARCHAR(100),
    bug_rbc VARCHAR(100),
    liens_pai VARCHAR(255),
    pilote VARCHAR(100),
    echeance DATE,
	_batch_id TEXT REFERENCES bronze.ingest_runs(batch_id),  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS bronze.ingest_runs;
CREATE TABLE bronze.ingest_runs(
	batch_id VARCHAR(100) PRIMARY KEY,
	start_time TIMESTAMP WITHOUT TIME ZONE,
	end_time TIMESTAMP WITHOUT TIME ZONE,
	status VARCHAR(100),
	total_files INT,
	total_rows INT,
	error_message TEXT
);

DROP TABLE IF EXISTS bronze.ingest_files;
CREATE TABLE bronze.ingest_files(
	file_id INT PRIMARY KEY,
	batch_id VARCHAR(100) REFERENCES bronze.ingest_runs(batch_id),
	source_system VARCHAR(100),
	processed_at TIMESTAMP WITHOUT TIME ZONE,
	filename VARCHAR(255),
	row_count INT,
	status VARCHAR(100),
	error_message TEXT
);
