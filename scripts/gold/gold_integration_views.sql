CREATE OR REPLACE VIEW gold.dim_subscriber AS
WITH
all_subscriber AS (
	SELECT DISTINCT imsi, msisdn, imei, nid_engine FROM silver.exp_etcs_call
	WHERE imsi IS NOT NULL
	UNION
	SELECT DISTINCT imsi, msisdn, imei, nid_engine FROM silver.exp_transaction_tracing
	WHERE imsi IS NOT NULL
	UNION
	SELECT DISTINCT imsi, msisdn, imei, NULL::INTEGER FROM silver.exp_ho_tracing
	WHERE imsi IS NOT NULL
	UNION
	SELECT DISTINCT imsi, msisdn, NULL::BIGINT, NULL::INTEGER FROM silver.exp_vgcs_vbs_rec_tracing
	WHERE imsi IS NOT NULL
	UNION
	SELECT DISTINCT imsi, msisdn, NULL::BIGINT, nid_engine FROM silver.exp_subscriber_matrix
	WHERE imsi IS NOT NULL
),

subscriber_base AS (
	SELECT
		imsi,
		MAX(msisdn) AS msisdn,
		MAX(imei) AS imei,
		MAX(nid_engine) AS nid_engine
	FROM all_subscriber
	GROUP BY imsi
)

SELECT
	ROW_NUMBER() OVER (ORDER BY sb.imsi) AS subscriber_key,
	sb.imsi,
	sb.msisdn,
	sb.imei,
	sb.nid_engine,
	sm.last_imsi_time AS imsi_last_time,
	sm.last_msisdn_time AS msisdn_last_time
FROM subscriber_base sb
LEFT JOIN silver.exp_subscriber_matrix sm ON sb.imsi = sm.imsi;

CREATE OR REPLACE VIEW gold.fact_etcs_call AS
SELECT
	d.subscriber_key,
	ec.imsi,
	ec.start_time,
	ec.stop_time,
	ec.nid_engine,
	ec.nid_operational,
	ec.etcs_baseline,
	ec.system_version,
	ec.start_nid_c,
	ec.start_nid_bg,
	ec.stop_nid_c,
	ec.stop_nid_bg,
	ec.stop_d_lrbg,
	ec.gsmr_connected,
	ec.etcs_connected,
	ec.root_failure, 
	ec.end_domain,
	ec.protocol_layer,
	ec.end_event,
	ec.end_cause,
	ec.isdn_port_probe,
	ec.call_setup_duration AS call_setup_duration_ms,
	ec.transaction_duration AS transaction_duration_ms
FROM silver.exp_etcs_call ec
LEFT JOIN gold.dim_subscriber d ON ec.imsi = d.imsi;

CREATE OR REPLACE VIEW gold.fact_transaction AS
SELECT
	d.subscriber_key,
	t.imsi,
	t.start_time,
	t.stop_time,
	t.nid_engine,
	t.start_lac, 
	t.start_ci, 
	t.stop_lac, 
	t.stop_ci,
	t.tmsi,
	t.reallocated_tmsi,
	t.direction, 
	t.transaction_type, 
	t.transaction_subtype, 
	t.application_type,
	t.functional_number,
	t.dest_route_address, 
	t.gsmr_connected, 
	t.priority, 
	t.gb_ciphering_algo,
	t.root_failure, 
	t.protocol_layer,
	t.end_event, 
	t.end_cause,
	t.call_setup_duration AS call_setup_duration_ms,
	t.establishment_delay AS establishment_delay_ms,
	t.transaction_duration AS transaction_duration_ms
FROM silver.exp_transaction_tracing t
LEFT JOIN gold.dim_subscriber d ON t.imsi = d.imsi;

CREATE OR REPLACE VIEW gold.fact_group_call AS
SELECT
	d.subscriber_key,
	v.imsi,
	v.start_time, 
	v.stop_time,
	v.functional_number,
	v.gid, 
	v.area, 
	v.gcr,
	v.application_type, 
	v.transaction_type, 
	v.transaction_subtype,
	v.priority,
	v.start_lac,
	v.start_ci,
	v.end_user,
	v.sccp_success_numerator, 
	v.sccp_success_denominator,
	v.sccp_success_rate,
	v.cell_success_numerator,
	v.cell_success_denominator, 
	v.cell_success_rate,
	v.dispatcher_success_numerator,
	v.dispatcher_success_denominator, 
	v.dispatcher_success_rate,
	v.establishment_delay AS establishment_delay_ms,
	v.vgcs_duration AS vgcs_duration_ms
FROM silver.exp_vgcs_vbs_rec_tracing v
LEFT JOIN gold.dim_subscriber d ON v.imsi = d.imsi;

CREATE OR REPLACE VIEW gold.fact_handover AS
SELECT
	d.subscriber_key,
	h.imsi,
	h.start_time, 
	h.stop_time, 
	h.ho_start_time AS handover_start_time, 
	h.src_lac AS source_lac, 
	h.src_ci AS source_ci, 
	h.trg_lac AS target_lac, 
	h.trg_ci AS target_ci,
	h.ms_power, 
	h.call_type, 
	h.ho_type AS handover_type,
	h.ho_end_event AS handover_end_event, 
	h.ho_end_cause AS handover_end_cause, 
	h.ho_cause AS handover_cause,
	CAST(h.ho_duration * 1000 AS INTEGER) AS handover_duration_ms
FROM silver.exp_ho_tracing h
LEFT JOIN gold.dim_subscriber d ON h.imsi = d.imsi;

CREATE OR REPLACE VIEW gold.fact_frame_error AS
SELECT
	d.subscriber_key,
	e.start_time, 
	e.stop_time, 
	e.frame_error_time,
	e.nid_engine, 
	e.nid_operational,
	e.isdn_port_probe,
	e.last_nid_c, 
	e.last_nid_bg,
	e.m_level AS multiplexing_level, 
	e.m_mode multiplexing_mode, 
	e.direction,
	e.frame_error, 
	e.frame_error_retransmission_count
FROM silver.exp_hdlc_frame_errors e
LEFT JOIN gold.dim_subscriber d ON e.calling_number = d.msisdn;

CREATE OR REPLACE VIEW gold.fact_ertms_disconnect AS
SELECT
	d.subscriber_key,
	sp.nombre_ordre AS incident_order,
	sp.derniere_7_jours AS is_last_7_days, 
	sp.date AS disconnect_date, 
	sp.heure AS disconnect_time,
	sp.rame, 
	sp.numero_train,
	sp.motrice_cab, 
	sp.mrm, 
	sp.imei,
	sp.sens AS direction_sens, 
	sp.km, 
	sp.niveau_etcs AS etcs_level, 
	sp.evenement AS event_type,
	sp.intervalle,
	sp.cause_racine AS root_cause, 
	sp.analyse_smmrgv AS analysis_smmrgv, 
	sp.sous_systeme_mis_en_cause AS subsystem_at_fault,
	sp.rxqual AS rx_quality, 
	sp.rxlev AS rx_level, 
	sp.voisinage_10sec AS neighbors_10sec, 
	sp.com_dte_dce AS dte_dce_com,
	sp.ecart AS timing_offset, 
	sp.retransmission_trames_hdlc_t70 AS hdlc_t70_retransmissions,
	sp.alarmes_bts AS bts_alarms,
	sp.action, 
	sp.execution_ho AS ho_execution,
	sp.traite_par AS handled_by,
	sp.cire, 
	sp.ciers, 
	sp.acquittement_auc AS auc_acknowledgment, 
	sp.bug_rbc AS rbc_bug, 
	sp.liens_pai AS pai_links,
	sp.pilote AS pilot, 
	sp.echeance AS resolution_deadline
FROM silver.sp_ertms_disconnects sp
LEFT JOIN gold.dim_subscriber d ON sp.imei = d.imei;
