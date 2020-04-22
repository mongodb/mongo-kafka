--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.3
-- Dumped by pg_dump version 9.6.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: deployment; Type: SCHEMA; Schema: -; Owner: caas
--

CREATE USER caas;

CREATE SCHEMA deployment;

ALTER SCHEMA deployment OWNER TO caas;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner:
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner:
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

--
-- Name: cloud_id; Type: DOMAIN; Schema: public; Owner: caas
--

CREATE DOMAIN cloud_id AS character varying(16) NOT NULL;


ALTER DOMAIN cloud_id OWNER TO caas;

--
-- Name: physical_cluster_id; Type: DOMAIN; Schema: public; Owner: caas
--

CREATE DOMAIN physical_cluster_id AS character varying(32) NOT NULL;


ALTER DOMAIN physical_cluster_id OWNER TO caas;


--
-- Name: logical_cluster_id; Type: DOMAIN; Schema: public; Owner: caas
--

CREATE DOMAIN logical_cluster_id AS character varying(32) NOT NULL;


ALTER DOMAIN logical_cluster_id OWNER TO caas;


--
-- Name: physical_cluster_version; Type: DOMAIN; Schema: public; Owner: caas
--

CREATE DOMAIN physical_cluster_version AS character varying(32) NOT NULL;


ALTER DOMAIN physical_cluster_version OWNER TO caas;

--
-- Name: cp_component_id; Type: DOMAIN; Schema: public; Owner: caas
--

CREATE DOMAIN cp_component_id AS character varying(32) NOT NULL;


ALTER DOMAIN cp_component_id OWNER TO caas;

--
-- Name: environment_id; Type: DOMAIN; Schema: public; Owner: caas
--

CREATE DOMAIN environment_id AS character varying(32) NOT NULL;


ALTER DOMAIN environment_id OWNER TO caas;

--
-- Name: hash_function; Type: TYPE; Schema: public; Owner: caas
--

CREATE TYPE hash_function AS ENUM (
    'none',
    'bcrypt'
);


ALTER TYPE hash_function OWNER TO caas;

--
-- Name: k8s_cluster_id; Type: DOMAIN; Schema: public; Owner: caas
--

CREATE DOMAIN k8s_cluster_id AS character varying(32) NOT NULL;


ALTER DOMAIN k8s_cluster_id OWNER TO caas;

--
-- Name: network_id; Type: DOMAIN; Schema: public; Owner: caas
--

CREATE DOMAIN network_id AS character varying(32) NOT NULL;


ALTER DOMAIN network_id OWNER TO caas;

--
-- Name: region_id; Type: DOMAIN; Schema: public; Owner: caas
--

CREATE DOMAIN region_id AS character varying(32) NOT NULL;


ALTER DOMAIN region_id OWNER TO caas;

--
-- Name: sasl_mechanism; Type: TYPE; Schema: public; Owner: caas
--

CREATE TYPE sasl_mechanism AS ENUM (
    'PLAIN',
    'SCRAM-SHA-256',
    'SCRAM-SHA-512'
);


ALTER TYPE sasl_mechanism OWNER TO caas;

--
-- Name: account_id; Type: DOMAIN; Schema: public; Owner: caas
--

CREATE DOMAIN account_id AS character varying(32) NOT NULL;


ALTER DOMAIN account_id OWNER TO caas;

SET search_path = deployment, pg_catalog;

--
-- Name: account_num; Type: SEQUENCE; Schema: deployment; Owner: caas
--

CREATE SEQUENCE account_num
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE account_num OWNER TO caas;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: account; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE account (
    id public.account_id DEFAULT ('a-'::text || nextval('account_num'::regclass)) NOT NULL,
    name character varying(32) NOT NULL,
    config jsonb,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    deactivated boolean DEFAULT false NOT NULL,
    organization_id integer NOT NULL
);


ALTER TABLE account OWNER TO caas;

CREATE UNIQUE INDEX account_name_is_unique ON deployment.account USING btree (name, organization_id) WHERE (deactivated = FALSE);

INSERT INTO account (id, name, organization_id) VALUES ('t0', 'Internal', 0);

--
-- Name: api_key; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE api_key (
    id integer NOT NULL,
    key character varying(128) NOT NULL,
    hashed_secret character varying(128) NOT NULL,
    hash_function public.hash_function DEFAULT 'bcrypt'::public.hash_function NOT NULL,
    sasl_mechanism public.sasl_mechanism DEFAULT 'PLAIN'::public.sasl_mechanism NOT NULL,
    cluster_id public.logical_cluster_id,
    deactivated boolean DEFAULT false NOT NULL,
    description text DEFAULT('') NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    user_id integer DEFAULT 0 NOT NULL
);


ALTER TABLE api_key OWNER TO caas;

--
-- Name: api_key_id_seq; Type: SEQUENCE; Schema: deployment; Owner: caas
--

CREATE SEQUENCE api_key_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE api_key_id_seq OWNER TO caas;

--
-- Name: api_key_id_seq; Type: SEQUENCE OWNED BY; Schema: deployment; Owner: caas
--

ALTER SEQUENCE api_key_id_seq OWNED BY api_key.id;


--
-- Name: cloud; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE cloud (
    id public.cloud_id NOT NULL,
    config jsonb,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    name TEXT DEFAULT '' NOT NULL
);


ALTER TABLE cloud OWNER TO caas;

--
-- Name: physical_cluster_num; Type: SEQUENCE; Schema: deployment; Owner: caas
--

CREATE SEQUENCE physical_cluster_num
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;

  ALTER TABLE physical_cluster_num OWNER TO caas;

--
-- Name: physical_cluster; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE physical_cluster (
    id public.physical_cluster_id NOT NULL,
    k8s_cluster_id public.k8s_cluster_id,
    type character varying(32) NOT NULL,
    config jsonb,
    deactivated timestamp without time zone,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    status character varying(32) NOT NULL
);


ALTER TABLE physical_cluster OWNER TO caas;

--
-- Name: logical_cluster_num; Type: SEQUENCE; Schema: deployment; Owner: caas
--

CREATE SEQUENCE logical_cluster_num
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;

ALTER TABLE logical_cluster_num OWNER TO caas;

--
-- Name: logical_cluster; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE logical_cluster (
    id public.logical_cluster_id NOT NULL,
    name character varying(64) NOT NULL,
    physical_cluster_id public.physical_cluster_id NOT NULL,
    type character varying(32) NOT NULL,
    account_id public.account_id,
    config jsonb,
    deactivated timestamp without time zone,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    status_detail jsonb NOT NULL default '{}'::jsonb,
    status_modified timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE logical_cluster OWNER TO caas;

--
-- Name: cp_component; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE cp_component (
    id public.cp_component_id NOT NULL,
    default_version public.physical_cluster_version DEFAULT '0.0.7'::character varying,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE cp_component OWNER TO caas;

--
-- Name: environment; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE environment (
    id public.environment_id NOT NULL,
    config jsonb,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE environment OWNER TO caas;

--
-- Name: k8s_cluster_num; Type: SEQUENCE; Schema: deployment; Owner: caas
--

CREATE SEQUENCE k8s_cluster_num
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE k8s_cluster_num OWNER TO caas;

--
-- Name: k8s_cluster; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE k8s_cluster (
    id public.k8s_cluster_id DEFAULT ('k8s-'::text || nextval('k8s_cluster_num'::regclass)) NOT NULL,
    network_id public.network_id,
    config jsonb,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE k8s_cluster OWNER TO caas;

--
-- Name: network_num; Type: SEQUENCE; Schema: deployment; Owner: caas
--

CREATE SEQUENCE network_num
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE network_num OWNER TO caas;

--
-- Name: network; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE network (
    id public.network_id DEFAULT ('n-'::text || nextval('network_num'::regclass)) NOT NULL,
    provider_id character varying(512),
    cloud public.cloud_id,
    region public.region_id,
    environment public.environment_id,
    config jsonb,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE network OWNER TO caas;

--
-- Name: organization; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE organization (
    id integer NOT NULL,
    name character varying(32) NOT NULL,
    deactivated boolean DEFAULT false NOT NULL,
    plan jsonb NOT NULL DEFAULT('{}'),
    saml jsonb NOT NULL DEFAULT('{}'),
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE organization OWNER TO caas;

COPY organization (id, name, plan) FROM stdin;
0	Internal	{"billing": {"email": "caas-team@confluent.io", "method": "MANUAL", "interval": "MONTHLY", "accrued_this_cycle": "0", "stripe_customer_id": ""}, "tax_address": {"zip": "", "city": "", "state": "", "country": "", "street1": "", "street2": ""}, "product_level": "TEAM", "referral_code": ""}
\.


--
-- Name: organization_id_seq; Type: SEQUENCE; Schema: deployment; Owner: caas
--

CREATE SEQUENCE organization_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE organization_id_seq OWNER TO caas;

--
-- Name: organization_id_seq; Type: SEQUENCE OWNED BY; Schema: deployment; Owner: caas
--

ALTER SEQUENCE organization_id_seq OWNED BY organization.id;

--
-- Name: coupon; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE coupon (
    id TEXT NOT NULL,
    coupon_type INTEGER DEFAULT 0 NOT NULL,
    amount_off INTEGER DEFAULT 0 NOT NULL,
    percent_off INTEGER DEFAULT 0 NOT NULL,
    redeem_by TIMESTAMP WITHOUT TIME zone,
    times_redeemed INTEGER DEFAULT 0 NOT NULL,
    max_redemptions INTEGER DEFAULT 0 NOT NULL,
    duration_in_months INTEGER DEFAULT 0 NOT NULL,
    deactivated BOOL DEFAULT FALSE NOT NULL,
    created TIMESTAMP WITHOUT TIME zone DEFAULT now() NOT NULL,
    modified TIMESTAMP WITHOUT TIME zone DEFAULT now() NOT NULL
);

ALTER TABLE coupon OWNER TO caas;

--
-- Name: coupon_id_seq; Type: SEQUENCE; Schema: deployment; Owner: caas
--

CREATE SEQUENCE coupon_id_seq;

ALTER TABLE coupon_id_seq OWNER TO caas;

--
-- Name: coupon_id_seq; Type: SEQUENCE OWNED BY; Schema: deployment; Owner: caas
--

ALTER SEQUENCE coupon_id_seq OWNED BY coupon.id;

--
-- Name: event; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE event (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    resource_type integer,
    resource_id TEXT NOT null,
    action INTEGER NOT NULL,
    data jsonb NOT NULL DEFAULT('{}'),
    created TIMESTAMP WITHOUT TIME zone DEFAULT now() NOT NULL
);

ALTER TABLE event OWNER TO caas;

--
-- Name: region; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE region (
    id public.region_id NOT NULL,
    cloud public.cloud_id,
    config jsonb,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    name TEXT DEFAULT '' NOT NULL
);


ALTER TABLE region OWNER TO caas;

--
-- Name: users; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE users (
    id integer NOT NULL,
    email character varying(128) NOT NULL,
    service_name character varying(64) DEFAULT '' NOT NULL,
    service_description character varying(128) DEFAULT '' NOT NULL,
    service_account boolean DEFAULT false NOT NULL,
    first_name character varying(32) NOT NULL,
    last_name character varying(32) NOT NULL,
    password character varying(64) NOT NULL,
    organization_id integer NOT NULL,
    deactivated boolean DEFAULT false NOT NULL,
    verified timestamp without time zone DEFAULT timestamp '1970-01-01 00:00:00.00000'  NOT NULL,
    password_changed timestamp without time zone DEFAULT now() NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL
);

CREATE UNIQUE INDEX users_email_one_active ON deployment.users USING btree (email) WHERE (deactivated = FALSE);
CREATE UNIQUE INDEX users_service_name_is_unique ON deployment.users USING btree (service_name, organization_id) WHERE (service_account = TRUE AND deactivated = FALSE);


ALTER TABLE users OWNER TO caas;

INSERT INTO users (id, email, first_name, last_name, password, organization_id) VALUES (0, 'caas-team+internal@confluent.io', '', '', '$2a$10$P05oytzmNEfvpSWa0l.RpOa7vEund/Hdt0w88kaRLMIWvpSNGB33S', 0);

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: deployment; Owner: caas
--

CREATE SEQUENCE users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE users_id_seq OWNER TO caas;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: deployment; Owner: caas
--

ALTER SEQUENCE users_id_seq OWNED BY users.id;


--
-- Name: api_key id; Type: DEFAULT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY api_key ALTER COLUMN id SET DEFAULT nextval('api_key_id_seq'::regclass);

--
-- Name: billing_job; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE billing_job (
    id SERIAL PRIMARY KEY,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    month timestamp without time zone DEFAULT now() NOT NULL,
    status jsonb NOT NULL DEFAULT('{}'),
    charges jsonb NOT NULL DEFAULT('{}')
);


ALTER TABLE billing_job OWNER TO caas;

--
-- Name: roll; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE roll (
    id SERIAL PRIMARY KEY,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    status jsonb NOT NULL DEFAULT('{}'),
    request jsonb NOT NULL DEFAULT('{}'),
    clusters jsonb NOT NULL DEFAULT('{}')
);

ALTER TABLE roll OWNER TO caas;

--
-- Name: secret; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE secret (
    id SERIAL PRIMARY KEY,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    deactivated timestamp without time zone DEFAULT NULL,
    type TEXT DEFAULT '' NOT NULL,
    config jsonb DEFAULT '{}' NOT NULL
);

ALTER TABLE secret OWNER TO caas;

--
-- Name: task; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE task (
    id SERIAL PRIMARY KEY,
    run_date timestamp without time zone DEFAULT now() NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    type integer NOT NULL,
    status integer NOT NULL,
    message text DEFAULT('') NOT NULL,
    sub_tasks jsonb NOT NULL DEFAULT('{}')
);

ALTER TABLE task OWNER TO caas;

--
-- Name: usage; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE usage (
    id SERIAL PRIMARY KEY,
    logical_cluster_id public.logical_cluster_id,
    month TIMESTAMP WITHOUT TIME zone DEFAULT now() NOT NULL,
    metrics jsonb NOT NULL DEFAULT('{}'),
    modified TIMESTAMP WITHOUT TIME zone DEFAULT now() NOT NULL,
    created TIMESTAMP WITHOUT TIME zone DEFAULT now() NOT NULL
);

ALTER TABLE usage OWNER TO caas;

--
-- Name: price; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE price (
    id SERIAL PRIMARY KEY,
    price_table jsonb NOT NULL DEFAULT('{}'),
    effective_date TIMESTAMP WITHOUT TIME zone DEFAULT now() NOT NULL,
    modified TIMESTAMP WITHOUT TIME zone DEFAULT now() NOT NULL,
    created TIMESTAMP WITHOUT TIME zone DEFAULT now() NOT NULL
);

ALTER TABLE price OWNER TO caas;

--
-- Name: invoice; Type: TABLE; Schema: deployment; Owner: caas
--

CREATE TABLE invoice (
    id SERIAL PRIMARY KEY,
    organization_id integer NOT NULL,
    lines jsonb NOT NULL DEFAULT('{}'),
    currency TEXT DEFAULT '' NOT NULL,
    from_date timestamp without time zone,
    to_date timestamp without time zone,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL
);

ALTER TABLE invoice OWNER TO caas;

--
-- Name: organization id; Type: DEFAULT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY organization ALTER COLUMN id SET DEFAULT nextval('organization_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY users ALTER COLUMN id SET DEFAULT nextval('users_id_seq'::regclass);


--
-- Data for Name: account; Type: TABLE DATA; Schema: deployment; Owner: caas
--

COPY account (id, name, config, created, modified, deactivated, organization_id) FROM stdin;
\.

--
-- Name: api_key_id_seq; Type: SEQUENCE SET; Schema: deployment; Owner: caas
--

SELECT pg_catalog.setval('api_key_id_seq', 2, true);


--
-- Data for Name: cloud; Type: TABLE DATA; Schema: deployment; Owner: caas
--

COPY cloud (id, config, created, modified, name) FROM stdin;
local	{"dns_domain": "local", "internal_dns_domain": "internal"}	2017-06-22 13:50:24.561325	2017-06-22 13:50:24.561325	Local
aws	{"dns_domain": "aws.devel.cpdev.cloud", "internal_dns_domain": "aws.internal.devel.cpdev.cloud"}	2017-06-22 13:50:24.561325	2017-06-22 13:50:24.561325	AWS
gcp	{"dns_domain": "gcp.devel.cpdev.cloud", "internal_dns_domain": "gcp.internal.devel.cpdev.cloud"}	2017-06-22 13:50:24.561325	2017-06-22 13:50:24.561325	GCP
\.


--
-- Data for Name: cp_component; Type: TABLE DATA; Schema: deployment; Owner: caas
--

COPY cp_component (id, default_version, created, modified) FROM stdin;
kafka	0.3.0	2017-06-22 13:50:24.580803	2017-06-22 13:50:24.580803
zookeeper	0.3.0	2017-06-22 13:50:24.580803	2017-06-22 13:50:24.580803
\.


--
-- Data for Name: environment; Type: TABLE DATA; Schema: deployment; Owner: caas
--

COPY environment (id, config, created, modified) FROM stdin;
local	{}	2017-06-22 13:50:24.575041	2017-06-22 13:50:24.575041
devel	{}	2017-06-08 23:18:32.009539	2017-08-19 01:23:42.349148
private	{}	2018-10-10 11:00:00.000000	2018-10-10 11:00:00.000000
\.


--
-- Data for Name: k8s_cluster; Type: TABLE DATA; Schema: deployment; Owner: caas
--

COPY k8s_cluster (id, network_id, config, created, modified) FROM stdin;
k8s1	n1	{"name": "minikube", "caas_version": "0.4.0-7-gd799613", "is_schedulable": true, "img_pull_policy": "IfNotPresent"}	2017-06-22 13:50:41.818297	2017-06-22 13:50:41.818297
k8s2	n2	{"name": "k8s-mothership.us-west-2", "caas_version": "0.6.10", "img_pull_policy": "IfNotPresent"}	2017-06-27 22:46:27.267852	2017-06-27 22:46:27.267852
k8s3	n3	{"name": "k8s3.us-west-2", "caas_version": "0.6.10", "is_schedulable": true, "img_pull_policy": "IfNotPresent"}	2017-06-27 22:46:27.267852	2017-06-27 22:46:27.267852
k8s4	n4	{"name": "k8s4.us-central1", "caas_version": "0.6.10", "is_schedulable": true, "img_pull_policy": "IfNotPresent"}	2017-06-27 22:46:27.267852	2017-06-27 22:46:27.267852
k8s5	n5	{"name": "k8s5.us-west-2", "caas_version": "0.6.10", "is_schedulable": true, "img_pull_policy": "IfNotPresent"}	2017-06-27 22:46:27.267852	2017-06-27 22:46:27.267852
\.


--
-- Name: k8s_cluster_num; Type: SEQUENCE SET; Schema: deployment; Owner: caas
--

SELECT pg_catalog.setval('k8s_cluster_num', 1, true);


--
-- Data for Name: network; Type: TABLE DATA; Schema: deployment; Owner: caas
--

COPY network (id, provider_id, cloud, region, environment, config, created, modified) FROM stdin;
n1	minikube	local	local	local	{"zones": [{"id": "minikube", "name": "a"}], "docker_repo": "minikube", "image_prefix": "confluentinc"}	2017-06-22 13:50:29.53859	2017-06-22 13:50:29.53859
n2	vpc-958feff3	aws	us-west-2	devel	{"zones": [{"id": "us-west-2a", "name": "a"}, {"id": "us-west-2b", "name": "b"}, {"id": "us-west-2c", "name": "c"}], "docker_repo": "037803949979.dkr.ecr.us-west-2.amazonaws.com", "image_prefix": "confluentinc"}	2017-06-22 13:50:29.53859	2017-06-22 13:50:29.53859
n3	vpc-eff08497	aws	us-west-2	devel	{"zones": [{"id": "us-west-2a", "name": "a"}], "docker_repo": "037803949979.dkr.ecr.us-west-2.amazonaws.com", "image_prefix": "confluentinc"}	2017-06-22 13:50:29.53859	2017-06-22 13:50:29.53859
n4	k8s-sz-b1-us-central1	gcp	us-central1	devel	{"zones": [{"id": "us-central1-b", "name": "b"}], "docker_repo": "us.gcr.io", "image_prefix": "cc-devel"}	2018-03-23 20:28:16.819057	2018-03-23 20:28:16.819057
n5	vpc-abcdef12	aws	us-west-2	devel	{"zones": [{"id": "us-west-2a", "name": "a"},{"id": "us-west-2b", "name": "b"},{"id": "us-west-2c", "name": "c"}], "docker_repo": "037803949979.dkr.ecr.us-west-2.amazonaws.com", "image_prefix": "confluentinc"}	2017-06-22 13:50:29.53859	2017-06-22 13:50:29.53859
\.


--
-- Name: network_num; Type: SEQUENCE SET; Schema: deployment; Owner: caas
--

SELECT pg_catalog.setval('network_num', 1, true);



--
-- Name: organization_id_seq; Type: SEQUENCE SET; Schema: deployment; Owner: caas
--

SELECT pg_catalog.setval('organization_id_seq', 592, true);


--
-- Data for Name: region; Type: TABLE DATA; Schema: deployment; Owner: caas
--

COPY region (id, cloud, config, created, modified, name) FROM stdin;
local	local	{}	2017-06-22 13:50:24.567898	2017-06-22 13:50:24.567898	Local
us-west-2	aws	{}	2017-06-22 13:50:24.567898	2017-06-22 13:50:24.567898	US West (Oregon)
us-central1	gcp	{}	2017-06-22 13:50:24.567898	2017-06-22 13:50:24.567898	US Central
\.


--
-- Name: account_num; Type: SEQUENCE SET; Schema: deployment; Owner: caas
--

SELECT pg_catalog.setval('account_num', 589, true);


--
-- Data for Name: users; Type: TABLE DATA; Schema: deployment; Owner: caas
--

COPY users (id, email, first_name, last_name, organization_id, deactivated, created, modified) FROM stdin;
\.


--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: deployment; Owner: caas
--

SELECT pg_catalog.setval('users_id_seq', 2, true);


--
-- Name: api_key api_key_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY api_key
    ADD CONSTRAINT api_key_pkey PRIMARY KEY (id);


--
-- Name: cloud cloud_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY cloud
    ADD CONSTRAINT cloud_pkey PRIMARY KEY (id);


--
-- Name: physical_cluster physical_cluster_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY physical_cluster
    ADD CONSTRAINT physical_cluster_pkey PRIMARY KEY (id);

--
-- Name: logical_cluster logical_cluster_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY logical_cluster
    ADD CONSTRAINT logical_cluster_pkey PRIMARY KEY (id);


--
-- Name: cp_component cp_component_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY cp_component
    ADD CONSTRAINT cp_component_pkey PRIMARY KEY (id);


--
-- Name: environment environment_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY environment
    ADD CONSTRAINT environment_pkey PRIMARY KEY (id);


--
-- Name: k8s_cluster k8s_cluster_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY k8s_cluster
    ADD CONSTRAINT k8s_cluster_pkey PRIMARY KEY (id);


--
-- Name: network network_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY network
    ADD CONSTRAINT network_pkey PRIMARY KEY (id);


--
-- Name: network network_provider_id_uniq; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY network
    ADD CONSTRAINT network_provider_id_uniq UNIQUE (provider_id);


--
-- Name: organization organization_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY organization
    ADD CONSTRAINT organization_pkey PRIMARY KEY (id);


--
-- Name: region region_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY region
    ADD CONSTRAINT region_pkey PRIMARY KEY (id);


--
-- Name: account account_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY account
    ADD CONSTRAINT account_pkey PRIMARY KEY (id);

--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: event event_organization_id_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY event
    ADD CONSTRAINT event_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES organization(id);


--
-- Name: event event_user_id_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY event
    ADD CONSTRAINT event_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id);


--
-- Name: api_key api_key_user_id_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY api_key
    ADD CONSTRAINT api_key_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id);


--
-- Name: api_key api_key_account_id_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY api_key
    ADD CONSTRAINT api_key_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES logical_cluster(id);


--
-- Name: logical_cluster logical_cluster_account_id_fkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY logical_cluster
    ADD CONSTRAINT logical_cluster_account_id_fkey FOREIGN KEY (account_id) REFERENCES account(id);


--
-- Name: logical_cluster logical_cluster_physical_cluster_id_fkey; Type: CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY logical_cluster
    ADD CONSTRAINT logical_cluster_physical_cluster_id_fkey FOREIGN KEY (physical_cluster_id) REFERENCES physical_cluster(id);


--
-- Name: physical_cluster physical_cluster_k8s_cluster_id_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY physical_cluster
    ADD CONSTRAINT physical_cluster_k8s_cluster_id_fkey FOREIGN KEY (k8s_cluster_id) REFERENCES k8s_cluster(id);


--
-- Name: k8s_cluster k8s_cluster_network_id_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY k8s_cluster
    ADD CONSTRAINT k8s_cluster_network_id_fkey FOREIGN KEY (network_id) REFERENCES network(id);


--
-- Name: network network_cloud_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY network
    ADD CONSTRAINT network_cloud_fkey FOREIGN KEY (cloud) REFERENCES cloud(id);


--
-- Name: network network_environment_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY network
    ADD CONSTRAINT network_environment_fkey FOREIGN KEY (environment) REFERENCES environment(id);


--
-- Name: network network_region_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY network
    ADD CONSTRAINT network_region_fkey FOREIGN KEY (region) REFERENCES region(id);


--
-- Name: region region_cloud_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY region
    ADD CONSTRAINT region_cloud_fkey FOREIGN KEY (cloud) REFERENCES cloud(id);


--
-- Name: account account_organization_id_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY account
    ADD CONSTRAINT account_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES organization(id);


--
-- Name: users users_organization_id_fkey; Type: FK CONSTRAINT; Schema: deployment; Owner: caas
--

ALTER TABLE ONLY users
    ADD CONSTRAINT users_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES organization(id);


--
-- PostgreSQL database dump complete
--
