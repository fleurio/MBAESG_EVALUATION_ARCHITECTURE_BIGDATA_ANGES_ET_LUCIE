# Rapport Projet : Analyse des Offres d'Emploi LinkedIn avec Snowflake

## Contexte : 
A partir d'un jeu de données d'offres d'emplois publiées sur Linkedin, il nous a été demandé d'effectuer des analyses pertinentes telles que :

1. Top 10 des titres de postes les plus publiés par industrie ;
2. Top 10 des postes les mieux rémunérés par industrie ;
3. Répartition des offres d’emploi par taille d’entreprise ;
4. Répartition des offres d’emploi par secteur d’activité ;
5. Répartition des offres d’emploi par type d’emploi (temps plein, stage, temps partiel).

📄 Le livrable Attendu est un dépôt github détaillant :

1. Les commandes SQL utilisées, avec explications ;
2. Le code Streamlit pour chaque visualisation, accompagné des résultats obtenus ;
3. Les problèmes rencontrés et les solutions apportées ;
4. Des commentaires explicatifs pour chaque étape.

Afin de mettre en lumière les éléments ci-dessus, notre Rapport se déclinera en 08 points :  

I. Configuration de l'environnement & Accès S3 ;
II. Définition des formats et Ingestion (Couche BRONZE) ;
III. Nettoyage & Structuration (Couche SILVER) ;
IV. Création de la zone analytique (Couche GOLD) ; 
V. Analyse de données ;
VI. Application Streamlit ;
VII. Analyses et interprétations ;
VIII. Problèmes rencontrés et solutions apportées.



## I. Configuration de l'environnement & Accès S3

L'objectif ici est de préparer l'infrastructure dans Snowflake afin de pouvoir accéder aux données hébergées sur le bucket S3.

```
-------------------------------------------------------------------------
-- PHASE 1 : CONFIGURATION DE L'ENVIRONNEMENT & ACCÈS S3
-------------------------------------------------------------------------
-- L'objectif ici est de préparer l'infrastructure dans Snowflake afin de pouvoir accéder aux données hébergées sur le bucket S3.

-- 1. Création de la base de données dédiée au projet
-- Le IF NOT EXISTS évite les erreurs si vous relancez le script
CREATE DATABASE IF NOT EXISTS LINKEDIN;

-- 2. Création des Schémas pour organiser l'architecture Médaillon
-- BRONZE : Pour les données brutes (landing zone)
-- SILVER : Pour les données nettoyées et transformées
CREATE SCHEMA IF NOT EXISTS LINKEDIN.BRONZE;
CREATE SCHEMA IF NOT EXISTS LINKEDIN.SILVER;

-- 3. Création du Stage Externe
-- Ce "Stage" est un objet qui pointe vers le dossier S3 public.
-- C'est grâce à lui que Snowflake pourra "voir" les fichiers.
CREATE OR REPLACE STAGE LINKEDIN.BRONZE.LINKEDIN_STAGE
  URL = 's3://snowflake-lab-bucket/';

-- 4. Vérification de la connexion
-- Cette commande liste les fichiers présents sur le S3. 
-- On l'exécute pour confirmer qu'il y'a bien 'job_postings.csv', 'companies.json', etc.
LIST @LINKEDIN.BRONZE.LINKEDIN_STAGE;

```

## II. Définition des formats et Ingestion (Couche BRONZE)

L'objectif ici est de dire à Snowflake comment lire chaque type de fichier (CSV et JSON) et de les copier dans des tables d'accueil.

```
-------------------------------------------------------------------------
-- PHASE 2 : Définition des formats et Ingestion (Couche BRONZE).
-------------------------------------------------------------------------
-- L'objectif ici est de dire à Snowflake comment lire chaque type de fichier (CSV et JSON) et de les copier dans des tables d'accueil.

-- 1. Définition des Formats de Fichiers
-- On crée des objets de format pour ne pas avoir à répéter les réglages à chaque fois.
-- Création du format pour les fichiers CSV
CREATE OR REPLACE FILE FORMAT LINKEDIN.BRONZE.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'; -- Important pour les descriptions avec des virgules

-- Création du format pour les fichiers JSON
CREATE OR REPLACE FILE FORMAT LINKEDIN.BRONZE.JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE; -- Permet de lire chaque objet du tableau comme une ligne

-- 2. Création des Tables Bronze (CSV)
-- Nous créons les tables pour les fichiers CSV en utilisant principalement des types STRING pour garantir que tout soit importé sans erreur de typage à ce stade.

-- Table pour Job Postings
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_POSTINGS (
    job_id STRING, company_name STRING, title STRING, description STRING,
    max_salary STRING, med_salary STRING, min_salary STRING, pay_period STRING,
    formatted_work_type STRING, location STRING, applies STRING,
    original_listed_time STRING, remote_allowed STRING, views STRING,
    job_posting_url STRING, application_url STRING, application_type STRING,
    expiry STRING, closed_time STRING, formatted_experience_level STRING,
    skills_desc STRING, listed_time STRING, posting_domain STRING,
    sponsored STRING, work_type STRING, currency STRING, compensation_type STRING
);

-- Table pour Benefits
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.BENEFITS (
    job_id STRING, inferred STRING, type STRING
);

-- Table pour Employee Counts
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.EMPLOYEE_COUNTS (
    company_id STRING, employee_count STRING, follower_count STRING, time_recorded STRING
);

-- Table pour Job Skills
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_SKILLS (
    job_id STRING, skill_abr STRING
);

-- 3. Création des Tables Bronze (JSON)
-- Pour les fichiers JSON, on utilise une colonne unique de type VARIANT.

CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANIES (data VARIANT);
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_INDUSTRIES (data VARIANT);
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_SPECIALITIES (data VARIANT);
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_INDUSTRIES (data VARIANT);


-- 4. Chargement des données (COPY INTO)
-- On lance l'importation massive depuis le stage S3.

-- Import des CSV avec COPY INTO et Vérification avec SELECT
COPY INTO LINKEDIN.BRONZE.JOB_POSTINGS FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/job_postings.csv FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.CSV_FORMAT');
SELECT * FROM LINKEDIN.BRONZE.JOB_POSTINGS LIMIT 5;

COPY INTO LINKEDIN.BRONZE.BENEFITS FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/benefits.csv FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.CSV_FORMAT');
SELECT * FROM LINKEDIN.BRONZE.BENEFITS LIMIT 5;

COPY INTO LINKEDIN.BRONZE.EMPLOYEE_COUNTS FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/employee_counts.csv FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.CSV_FORMAT');
SELECT * FROM LINKEDIN.BRONZE.EMPLOYEE_COUNTS LIMIT 5;

COPY INTO LINKEDIN.BRONZE.JOB_SKILLS FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/job_skills.csv FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.CSV_FORMAT');
SELECT * FROM LINKEDIN.BRONZE.JOB_SKILLS LIMIT 5;

-- Import des JSON avec COPY INTO et Vérification avec SELECT
COPY INTO LINKEDIN.BRONZE.COMPANIES FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/companies.json FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.JSON_FORMAT');
SELECT * FROM LINKEDIN.BRONZE.COMPANIES LIMIT 5;

COPY INTO LINKEDIN.BRONZE.COMPANY_INDUSTRIES FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/company_industries.json FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.JSON_FORMAT');
SELECT * FROM LINKEDIN.BRONZE.COMPANY_INDUSTRIES LIMIT 5;

COPY INTO LINKEDIN.BRONZE.COMPANY_SPECIALITIES FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/company_specialities.json FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.JSON_FORMAT');
SELECT * FROM LINKEDIN.BRONZE.COMPANY_SPECIALITIES LIMIT 5;

COPY INTO LINKEDIN.BRONZE.JOB_INDUSTRIES FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/job_industries.json FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.JSON_FORMAT');
SELECT * FROM LINKEDIN.BRONZE.JOB_INDUSTRIES LIMIT 5;

```

## III. Nettoyage & Structuration (Couche SILVER)

Ici on va transformer le JSON brut en colonnes lisibles et convertir les textes en nombres ou en dates pour permettre les calculs.

```
-------------------------------------------------------------------------
-- PHASE 3 : NETTOYAGE & STRUCTURATION (SILVER)
-------------------------------------------------------------------------
-- Ici on va transformer le JSON brut en colonnes lisibles et convertir les textes en nombres ou en dates pour permettre les calculs.

-- 1. "Aplatir" les fichiers JSON
-- Nous allons extraire les données des colonnes VARIANT pour créer des tables relationnelles propres.

-- a. Transformation de la table COMPANIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANIES AS
SELECT
    data:company_id::INT AS company_id,
    data:name::STRING AS name,
    data:company_size::INT AS company_size,
    data:state::STRING AS state,
    data:country::STRING AS country,
    data:city::STRING AS city,
    data:zip_code::STRING AS zip_code,
    data:url::STRING AS linkedin_url
FROM LINKEDIN.BRONZE.COMPANIES;
--Vérification
SELECT * FROM LINKEDIN.SILVER.COMPANIES LIMIT 5;

-- b. Transformation de JOB_INDUSTRIES (Lien entre Job et Secteur)
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_INDUSTRIES AS
SELECT
    data:job_id::INT AS job_id,
    data:industry_id::INT AS industry_id
FROM LINKEDIN.BRONZE.JOB_INDUSTRIES;
--Vérification
SELECT * FROM LINKEDIN.SILVER.JOB_INDUSTRIES LIMIT 5;

-- c. Transformation de COMPANY_INDUSTRIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_INDUSTRIES AS
SELECT
    data:company_id::INT AS company_id,
    data:industry::STRING AS industry_name
FROM LINKEDIN.BRONZE.COMPANY_INDUSTRIES;
--Vérification
SELECT * FROM LINKEDIN.SILVER.COMPANY_INDUSTRIES LIMIT 5;


-- 2. Nettoyage de la table principale (JOB_POSTINGS)
-- Ici, on s'occupe de convertir les salaires et les dates, car dans la couche Bronze, tout est en STRING.

CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_POSTINGS AS
SELECT
    job_id::INT AS job_id,
    company_name,
    title,
    -- Nettoyage des salaires : on convertit en FLOAT pour les calculs
    NULLIF(max_salary, 'null')::FLOAT AS max_salary,
    NULLIF(med_salary, 'null')::FLOAT AS med_salary,
    NULLIF(min_salary, 'null')::FLOAT AS min_salary,
    pay_period,
    location,
    applies::INT AS nb_applications,
    views::INT AS nb_views,
    -- Conversion des dates (souvent en millisecondes dans les exports LinkedIn)
    TO_TIMESTAMP_NTZ(listed_time::BIGINT / 1000) AS listed_at,
    work_type,
    formatted_experience_level,
    currency
FROM LINKEDIN.BRONZE.JOB_POSTINGS;
--Vérification
SELECT * FROM LINKEDIN.SILVER.JOB_POSTINGS LIMIT 5;


-- 3. Nettoyage des tables annexes (CSV)
-- On fait de même pour les Bénéfices, les compétences et les statistiques d'employés.

-- Table SILVER.BENEFITS 
CREATE OR REPLACE TABLE LINKEDIN.SILVER.BENEFITS AS
SELECT
    job_id::INT AS job_id,
    inferred::BOOLEAN AS is_inferred, -- "True/False" devient un vrai type Booléen
    type AS benefit_type
FROM LINKEDIN.BRONZE.BENEFITS;
-- Vérification rapide
SELECT * FROM LINKEDIN.SILVER.BENEFITS LIMIT 5;

-- Table des compétences
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_SKILLS AS
SELECT
    job_id::INT AS job_id,
    skill_abr
FROM LINKEDIN.BRONZE.JOB_SKILLS;
--Vérification
SELECT * FROM LINKEDIN.SILVER.JOB_SKILLS LIMIT 5;

-- Table des statistiques employés
CREATE OR REPLACE TABLE LINKEDIN.SILVER.EMPLOYEE_COUNTS AS
SELECT
    company_id::INT AS company_id,
    employee_count::INT AS employee_count,
    follower_count::INT AS follower_count,
    TO_TIMESTAMP_NTZ(time_recorded::BIGINT) AS recorded_at
FROM LINKEDIN.BRONZE.EMPLOYEE_COUNTS;
--Vérification
SELECT * FROM LINKEDIN.SILVER.EMPLOYEE_COUNTS LIMIT 5;

```

## IV. Création de la zone analytique (Couche GOLD)

L'objectif ici est de construire les tables et vues "métier" qui répondent directement aux questions du projet.

```
-------------------------------------------------------------------------
-- PHASE 4 : CRÉATION DU SCHÉMA GOLD (ZONE ANALYTIQUE)
-------------------------------------------------------------------------
-- L'objectif ici est de construire les tables et vues "métier" qui répondent directement aux questions du projet.

-- 1. Création du Schéma GOLD
CREATE SCHEMA IF NOT EXISTS LINKEDIN.GOLD;

-- 2. Création de la table de faits (Fact Table)
-- Pour faciliter nos analyses (Top 10, répartitions), il est très utile de créer une table centrale "augmentée" qui regroupe les informations de plusieurs tables.
-- Cette table regroupe l'essentiel pour les analyses de salaires et d'industries
CREATE OR REPLACE TABLE LINKEDIN.GOLD.FACT_JOB_POSTINGS AS
SELECT 
    j.job_id,
    j.title,
    j.company_name,
    j.max_salary,
    j.min_salary,
    j.location,
    j.work_type,
    j.formatted_experience_level,
    ji.industry_id,
    ci.industry_name
FROM LINKEDIN.SILVER.JOB_POSTINGS j
LEFT JOIN LINKEDIN.SILVER.JOB_INDUSTRIES ji ON j.job_id = ji.job_id
LEFT JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci ON ji.industry_id = ci.company_id; -- Note : Vérifiez si la jointure se fait sur ID ou Nom selon vos fichiers
-- Vérification
SELECT * FROM LINKEDIN.GOLD.FACT_JOB_POSTINGS LIMIT 5;

```

## V. Requêtes et Analyse de données

L'objectif ici est de traduire les objectifs métiers en requêtes SQL performantes.

```
-------------------------------------------------------------------------
-- PHASE 5 : ANALYSE DE DONNEES
-------------------------------------------------------------------------
-- Nous allons traduire les objectifs métiers en requêtes SQL performantes.

-- 1 : Top 10 des titres de postes les plus publiés par industrie
-- L'objectif : Identifier quels métiers recrutent le plus dans chaque secteur.
-- La stratégie : Nous devons joindre JOB_POSTINGS à JOB_INDUSTRIES pour obtenir le lien avec l'industrie, puis compter les occurrences.

WITH JobCount AS (
-- WITH JobCount AS est une CTE (Common Table Expression). C'est comme une table temporaire qui rend le code beaucoup plus lisible.
    SELECT 
        ci.industry_name,
        j.title,
        COUNT(*) AS nb_offres,
        ROW_NUMBER() OVER (
            PARTITION BY ci.industry_name 
            ORDER BY COUNT(*) DESC
        ) as rang
    FROM LINKEDIN.SILVER.JOB_POSTINGS j
    -- On lie d'abord le job à l'entreprise
    JOIN LINKEDIN.SILVER.COMPANIES c 
        ON j.company_name::INT = c.company_id
    -- On lie ensuite l'entreprise à son secteur
    JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci 
        ON c.company_id = ci.company_id
    GROUP BY ci.industry_name, j.title
)
SELECT 
    industry_name,
    title,
    nb_offres
FROM JobCount
WHERE rang <= 10 -- On ne garde que les 10 meilleurs
ORDER BY industry_name, nb_offres DESC;


-- 2 : Top 10 des postes les mieux rémunérés par industrie

WITH UniqueSalaries AS (
    -- Étape A : On récupère des lignes uniques (Titre/Salaire/Secteur)
    SELECT DISTINCT
        ci.industry_name,
        j.title,
        j.max_salary,
        j.currency
    FROM LINKEDIN.SILVER.JOB_POSTINGS j
    JOIN LINKEDIN.SILVER.COMPANIES c ON j.company_name::INT = c.company_id
    JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci ON c.company_id = ci.company_id
    WHERE j.max_salary IS NOT NULL AND j.max_salary > 0
),
RankedSalaries AS (
    -- Étape B : On applique le rang sur ces lignes déjà uniques
    SELECT 
        industry_name,
        title,
        max_salary,
        currency,
        ROW_NUMBER() OVER (
            PARTITION BY industry_name 
            ORDER BY max_salary DESC
        ) as rang
    FROM UniqueSalaries
)
SELECT 
    industry_name,
    title,
    max_salary,
    currency
FROM RankedSalaries
WHERE rang <= 10
ORDER BY industry_name, max_salary DESC;


-- 3 : Répartition des offres par taille de l'entreprise

SELECT 
    CASE 
        WHEN c.company_size = 1 THEN '1. Très petite (1-10 emp.)'
        WHEN c.company_size = 2 THEN '2. Petite (11-50 emp.)'
        WHEN c.company_size = 3 THEN '3. Moyenne (51-200 emp.)'
        WHEN c.company_size = 4 THEN '4. Grande (201-500 emp.)'
        WHEN c.company_size = 5 THEN '5. Très grande (501-1000 emp.)'
        WHEN c.company_size = 6 THEN '6. Enterprise (1001-5000 emp.)'
        WHEN c.company_size = 7 THEN '7. Multinationale (5000+ emp.)'
        ELSE '0. Non spécifié'
    END AS categorie_taille,
    COUNT(*) AS nb_offres
FROM LINKEDIN.SILVER.JOB_POSTINGS j
-- On joint le "faux" name (qui est un ID) avec le vrai company_id
JOIN LINKEDIN.SILVER.COMPANIES c ON j.company_name::INT = c.company_id --En effet dans company_name de J on a stocké l'ID au lieu du nom de C en texte (avec un .0 à la fin. Taper ceci pour vérifierSELECT DISTINCT company_name FROM LINKEDIN.SILVER.JOB_POSTINGS LIMIT 5;), on le convertit en entier pour qu'il soit identique au company_id de la table des entreprises.
GROUP BY c.company_size
ORDER BY c.company_size ASC;


-- 4 : Répartition des offres par secteur d'activité

SELECT 
    ci.industry_name,
    COUNT(j.job_id) AS nb_offres
FROM LINKEDIN.SILVER.JOB_POSTINGS j
-- 1. On lie le Job à l'entreprise (via l'ID numérique qu'on a découvert)
JOIN LINKEDIN.SILVER.COMPANIES c 
    ON j.company_name::INT = c.company_id
-- 2. On lie l'entreprise à ses secteurs
JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci 
    ON c.company_id = ci.company_id
GROUP BY ci.industry_name
ORDER BY nb_offres DESC;



-- 5 : Répartition par type d'offres par type d’emploi (temps plein, stage, temps partiel).

SELECT 
    CASE 
        WHEN work_type = 'FULL_TIME' THEN 'Temps Plein'
        WHEN work_type = 'PART_TIME' THEN 'Temps Partiel'
        WHEN work_type = 'CONTRACT' THEN 'Contrat / Freelance'
        WHEN work_type = 'INTERNSHIP' THEN 'Stage'
        WHEN work_type = 'TEMPORARY' THEN 'Temporaire'
        WHEN work_type = 'OTHER' THEN 'Autre'
        ELSE 'Non spécifié'
    END AS type_contrat,
    COUNT(*) AS nb_offres
FROM LINKEDIN.SILVER.JOB_POSTINGS
GROUP BY work_type
ORDER BY nb_offres DESC;

```

## VI. Application Streamlit

L'objectif ici est de concevoir une application permettant de visualiser de manière interactive les résultats des analyses effectuées ci-haut.
Pour rendre le rapport interactif, nous avons implémenté :
* Le Caching (@st.cache_data) : Pour réduire le temps de latence et la consommation de ressources Snowflake.
* La Visualisation Dynamique : Utilisation de Plotly pour offrir des graphiques interactifs (survol, zoom) plutôt que des images statiques.

```
# =========================================================================
# APPLICATION STREAMLIT
# =========================================================================

import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px

# =========================================================================
# CONFIGURATION INITIALE & CONNEXION
# =========================================================================
st.set_page_config(layout="wide", page_title="Dashboard LinkedIn")
st.title("📊 Analyse du Marché de l'Emploi LinkedIn")
st.write("Ce dashboard présente une analyse multidimensionnelle des offres d'emploi LinkedIn.")

# Récupération de la session active Snowflake (Spécifique à SiS)
session = get_active_session()

# On cache la liste des secteurs pour éviter de recharger la requête à chaque clic
@st.cache_data  # Cela rend l'application plus rapide en évitant de ré-interroger Snowflake pour des données qui ne changent pas (comme la liste des secteurs).

def load_industry_list():
    return session.sql("SELECT DISTINCT industry_name FROM LINKEDIN.SILVER.COMPANY_INDUSTRIES ORDER BY industry_name").to_pandas()

# =========================================================================
# FILTRE INTERACTIF (Utilisé pour les Questions 1 et 2)
# =========================================================================
# Menu déroulant permettant une visualisation dynamique. Les Question 1 & 2 changent les graphiques en temps réel. 
industries = load_industry_list()
selected_industry = st.selectbox("🎯 Sélectionnez un secteur pour affiner l'analyse :", industries['INDUSTRY_NAME'])

st.divider()

# =========================================================================
# QUESTIONS 1 & 2 : ANALYSE DÉTAILLÉE PAR SECTEUR
# =========================================================================
col1, col2 = st.columns(2)

with col1:
    # --- QUESTION 1 : TOP 10 DES MÉTIERS PAR SECTEUR ---
    st.header("1. Métiers les plus recherchés")
    q1 = f"""
    SELECT title, COUNT(*) as nb_offres
    FROM LINKEDIN.SILVER.JOB_POSTINGS j
    JOIN LINKEDIN.SILVER.COMPANIES c ON j.company_name::INT = c.company_id
    JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci ON c.company_id = ci.company_id
    WHERE ci.industry_name = '{selected_industry}'
    GROUP BY title ORDER BY nb_offres DESC LIMIT 10
    """
    df1 = session.sql(q1).to_pandas()
    # Visualisation Q1 : Graphique à barres horizontales
    fig1 = px.bar(df1, x='NB_OFFRES', y='TITLE', orientation='h', 
                  title=f"Top 10 des intitulés en {selected_industry}",
                  labels={'TITLE': 'Métier', 'NB_OFFRES': 'Nombre d\'offres'})
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    # --- QUESTION 2 : TOP 10 DES SALAIRES PAR SECTEUR ---
    st.header("2. Plus hauts salaires proposés")
    q2 = f"""
    SELECT DISTINCT title, max_salary
    FROM LINKEDIN.SILVER.JOB_POSTINGS j
    JOIN LINKEDIN.SILVER.COMPANIES c ON j.company_name::INT = c.company_id
    JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci ON c.company_id = ci.company_id
    WHERE ci.industry_name = '{selected_industry}' AND max_salary > 0
    ORDER BY max_salary DESC LIMIT 10
    """
    df2 = session.sql(q2).to_pandas()
    # Visualisation Q2 : Graphique à barres (couleur différente pour distinguer les salaires)
    fig2 = px.bar(df2, x='MAX_SALARY', y='TITLE', orientation='h',
                  color_discrete_sequence=['#26a69a'],
                  title=f"Top 10 des salaires max en {selected_industry}",
                  labels={'TITLE': 'Métier', 'MAX_SALARY': 'Salaire Max ($)'})
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# =========================================================================
# QUESTIONS 3, 4 & 5 : ANALYSE GLOBALE DU MARCHÉ
# =========================================================================
st.title("📈 Statistiques Globales (Toutes industries confondues)")
c3, c4, c5 = st.columns(3)

with c3:
    # --- QUESTION 3 : RÉPARTITION PAR TAILLE D'ENTREPRISE ---
    st.subheader("3. Structure des employeurs")
    q3 = """
    SELECT 
        CASE 
            WHEN company_size = 7 THEN '7. Multinationales'
            WHEN company_size >= 5 THEN '5-6. Grandes Entreprises'
            ELSE '1-4. PME/TPE'
        END as TAILLE,
        COUNT(*) as NB
    FROM LINKEDIN.SILVER.JOB_POSTINGS j
    JOIN LINKEDIN.SILVER.COMPANIES c ON j.company_name::INT = c.company_id
    GROUP BY 1 ORDER BY 1
    """
    df3 = session.sql(q3).to_pandas()
    # Visualisation Q3 : Graphique en anneau (Donut Chart)
    fig3 = px.pie(df3, values='NB', names='TAILLE', hole=0.4, title="Répartition par taille")
    st.plotly_chart(fig3, use_container_width=True)

with c4:
    # --- QUESTION 4 : RÉPARTITION PAR SECTEUR D'ACTIVITÉ ---
    st.subheader("4. Secteurs les plus actifs")
    q4 = """
    SELECT ci.industry_name, COUNT(*) as nb_offres
    FROM LINKEDIN.SILVER.JOB_POSTINGS j
    JOIN LINKEDIN.SILVER.COMPANIES c ON j.company_name::INT = c.company_id
    JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci ON c.company_id = ci.company_id
    GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """
    df4 = session.sql(q4).to_pandas()
    # Visualisation Q4 : Classement des secteurs d'activité
    fig4 = px.bar(df4, x='NB_OFFRES', y='INDUSTRY_NAME', orientation='h',
                  title="Top 10 des secteurs qui recrutent",
                  labels={'INDUSTRY_NAME': 'Secteur', 'NB_OFFRES': 'Offres'})
    st.plotly_chart(fig4, use_container_width=True)

with c5:
    # --- QUESTION 5 : RÉPARTITION PAR TYPE DE CONTRAT ---
    st.subheader("5. Modalités de travail")
    q5 = """
    SELECT work_type, COUNT(*) as nb
    FROM LINKEDIN.SILVER.JOB_POSTINGS
    GROUP BY 1 ORDER BY 2 DESC
    """
    df5 = session.sql(q5).to_pandas()
    # Visualisation Q5 : Camembert (Pie Chart) pour les types de contrats
    fig5 = px.pie(df5, values='NB', names='WORK_TYPE', title="Répartition des contrats")
    st.plotly_chart(fig5, use_container_width=True)

```

## VII. Analyses et interprétations

La dernière étape a consisté à valider la cohérence des graphiques avec la réalité du marché de l'emploi LinkedIn (ex: prédominance du secteur "Computer Software", importance des PME dans le volume d'offres).

### 1. Analyse de la demande : Domination des secteurs technologiques
L'analyse du Top 10 des secteurs (Question 4) révèle généralement une prédominance du secteur "Computer Software" et des services technologiques.

** Interprétation : ** Cela confirme que la transformation digitale reste le principal moteur de l'emploi mondial. Pour un expert en stratégie digitale (votre profil), cela montre que les opportunités ne se limitent pas aux entreprises de la Tech, mais s'étendent à tous les secteurs qui recrutent ces compétences pour se moderniser.

### 2. Analyse des salaires : Corrélation entre rareté et expertise
En observant les Salaires par secteur (Question 2), on remarque souvent que les titres de postes les plus fréquents ne sont pas forcément les mieux payés.

** Interprétation : ** Les salaires les plus élevés se concentrent sur des rôles de niche (Data Architects, Cybersecurity Managers) ou des rôles de direction. Cela illustre la tension sur le marché : les entreprises sont prêtes à offrir des primes significatives pour des compétences techniques pointues que le système de formation peine à fournir en volume.

### 3. Analyse de la structure du marché : Le poids des PME vs Multinationales
Le graphique sur la Taille d'entreprise (Question 3) permet de voir qui recrute réellement.

** Interprétation : ** Si les multinationales captent souvent l'attention médiatique, les PME/TPE représentent souvent une part massive (souvent plus de 50%) du volume d'offres. Pour une stratégie de recherche d'emploi ou d'alternance, cela signifie qu'il est crucial de ne pas cibler uniquement les "grands noms" mais d'explorer le tissu des entreprises de taille intermédiaire, souvent plus agiles.

### 4. Analyse des modes de travail : La mutation des contrats
L'analyse des Types de contrats (Question 5) met en lumière la flexibilité du marché.

** Interprétation : ** La présence importante du "Full-time" mixé à une montée du "Contract" ou du "Remote" (selon les données de la table work_type) indique une hybridation du travail. Les entreprises cherchent de plus en plus de la flexibilité, ce qui favorise l'émergence de missions de consulting ou de freelancing au sein même des structures salariées classiques.

### 5. Analyse de l'évolution des métiers : Le "Skill Gap"
En comparant les Intitulés de postes (Question 1), on voit apparaître des métiers hybrides (ex: Marketing Automation Manager, Data-Driven Marketer).

** Interprétation : ** Le marché ne cherche plus seulement des "généralistes", mais des profils capables de faire le pont entre le business et la technologie. Nous remarquons que c'est le cœur même de notre formation MBA : l'innovation technologique au service de la stratégie.



## VIII. Problèmes rencontrés et solutions apportées

Dans cette section, nous mettons en avant la transition de la donnée brute (Silver) vers la donnée exploitable (Gold).

### 1. Problème de granularité et "effet entonnoir" (Requêtes 1 & 2)
** Problème : ** Lors des premières jointures entre les offres d'emploi et les secteurs d'activité, le volume de données chutait de manière anormale (seulement 4 secteurs affichés sur des centaines).

** Cause : ** Une erreur de clé de jointure. Nous tentions de lier l'ID du secteur (industry_id) avec l'ID de l'entreprise (company_id), deux valeurs numériques sans relation logique.

** Solution : ** Reconstruction de la "chaîne de confiance" relationnelle. Nous avons identifié que le champ company_name de la table des offres contenait en réalité l'ID numérique de l'entreprise. La jointure a été corrigée pour passer par la table pivot COMPANY_INDUSTRIES en utilisant company_id.

### 2. Doublons de lignes lors des jointures (Requête 2)
** Problème : ** Le Top 10 des salaires affichait parfois 10 fois la même ligne pour un seul secteur (ex: "Accounting").

** Cause : ** La structure de la table des secteurs d'entreprise générait des produits cartésiens (doublons) lors de la jointure, et la fonction de fenêtrage ROW_NUMBER() attribuait un rang différent à des lignes identiques.

** Solution : ** Utilisation d'une CTE (Common Table Expression) intermédiaire incluant une clause DISTINCT pour nettoyer les données avant d'appliquer le classement statistique.

### 3. Configuration de l'environnement applicatif (Streamlit)
** Problème : ** Erreur ModuleNotFoundError lors de l'importation de bibliothèques de visualisation (Plotly).

** Cause : ** L'environnement isolé de Snowflake (SiS) ne charge pas par défaut les bibliothèques tierces.

** Solution : ** Déclaration explicite des dépendances dans le gestionnaire de paquets (Packages) de l'interface Snowflake pour inclure plotly et pandas.












**Snowflake Cortex AI** : Snowflake Cortex AI est un service entièrement managé conçu pour exploiter le potentiel de cette technologie auprès de tous les collaborateurs d’une organisation, quels que soient leurs niveaux de compétences techniques. Il donne accès à des modèles de langage de grande taille (LLM) de premier plan, permettant aux utilisateurs de concevoir et de déployer facilement des applications basées sur l’IA.

Dans cet exercice, nous explorons Cortex Analyst, l’une des fonctionnalités phares de la famille Snowflake Cortex AI.

**Cortex Analyst :**
Cortex Analyst permet aux utilisateurs métiers d’interagir avec des données structurées en langage naturel, afin d’obtenir des réponses plus rapidement, d’accéder à des analyses en libre-service et de gagner un temps précieux.

Commençons par un exemple simple utilisant la fonction **TRANSLATE** :

```
select SNOWFLAKE.CORTEX.TRANSLATE('Hello Everyone', '', 'fr') AS greeting;
```

La colonne REVIEW_TEXT de FACT_REVIEWS contient des avis dans différentes langues. Commençons par les traduire en anglais afin de vérifier si le sentiment correspond bien au contenu de l’avis.

* Créez un nouvel entrepôt (Warehouse) avec davantage de ressources de calcul.

```
CREATE OR REPLACE WAREHOUSE XLARGE_COMPUTE_WH WITH
COMMENT = 'Large warehouse for cortex analyst'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xlarge'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = true
    INITIALLY_SUSPENDED = true;
```
 
```
USE WAREHOUSE XLARGE_COMPUTE_WH;
```

```
CREATE OR REPLACE TABLE AIRBNB.GOLD.FULL_MOON_REVIEWS_TRANSLATED AS
SELECT 
    LISTING_ID,
    REVIEW_DATE, 
    REVIEWER_NAME, 
    REVIEW_TEXT, 
    SNOWFLAKE.CORTEX.TRANSLATE(REVIEW_TEXT, '', 'en') AS TRANSLATED_REVIEW_TEXT,
    SENTIMENT,
    IS_FULL_MOON  
    FROM AIRBNB.GOLD.FULL_MOON_REVIEWS
    LIMIT 100;
```

* Ensuite, nous utiliserons les modèles pré-entraînés de Snowflake Cortex pour générer des scores de sentiment pour chaque avis.

```
CREATE OR REPLACE TABLE AIRBNB.GOLD.FULL_MOON_REVIEWS_AUGMENTED AS
SELECT 
    LISTING_ID,
    REVIEW_DATE, 
    REVIEWER_NAME, 
    TRANSLATED_REVIEW_TEXT,
    SENTIMENT,
    SNOWFLAKE.CORTEX.SENTIMENT(TRANSLATED_REVIEW_TEXT) AS SENTIMENT_GENERATED,
    IS_FULL_MOON  
    FROM AIRBNB.GOLD.FULL_MOON_REVIEWS_TRANSLATED;
```

* Les scores de sentiment vont de -1 (totalement négatif) à 1 (totalement positif). Nous les classerons en trois catégories : négatif (-1 à -0,3), neutre (-0,3 à 0,3) et positif (0,3 à 1).

```
select min(SENTIMENT_GENERATED), max(SENTIMENT_GENERATED) from AIRBNB.GOLD.FULL_MOON_REVIEWS_AUGMENTED;
```


```
CREATE OR REPLACE TABLE AIRBNB.GOLD.FULL_MOON_REVIEWS_AUGMENTED AS
SELECT 
    LISTING_ID,
    REVIEW_DATE, 
    REVIEWER_NAME, 
    TRANSLATED_REVIEW_TEXT,
    SENTIMENT,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(TRANSLATED_REVIEW_TEXT) < -0.3 THEN 'negative'
     
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(TRANSLATED_REVIEW_TEXT) BETWEEN -0.3 AND 0.3  THEN 'neutral'
     
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(TRANSLATED_REVIEW_TEXT) > 0.3 THEN 'positive'
    END AS  SENTIMENT_GENERATED,
    IS_FULL_MOON  
    FROM AIRBNB.GOLD.FULL_MOON_REVIEWS_TRANSLATED;
```

* Interrogez la table `FULL_MOON_REVIEWS_AUGMENTED`.

```
SELECT * FROM AIRBNB.GOLD.FULL_MOON_REVIEWS_AUGMENTED
```

* Dans les exemples suivants, nous montrerons comment exploiter les fonctions `EXTRACT_ANSWER` et `SUMMARIZE` afin d’obtenir davantage d’informations et d’analyses à partir de nos données.

* EXTRACT_ANSWER:

```
select TRANSLATED_REVIEW_TEXT,SENTIMENT_GENERATED, SNOWFLAKE.CORTEX.EXTRACT_ANSWER(TRANSLATED_REVIEW_TEXT, 'Were the guests satisfied with their stay?') as extract_answer from AIRBNB.GOLD.FULL_MOON_REVIEWS_AUGMENTED limit 100;
```

* SUMMARIZE:

```
select TRANSLATED_REVIEW_TEXT, SNOWFLAKE.CORTEX.SUMMARIZE(TRANSLATED_REVIEW_TEXT) as Summary from AIRBNB.GOLD.FULL_MOON_REVIEWS_AUGMENTED limit 100;
```

## Créer des applications web de données :
Streamlit est une bibliothèque Python open source qui facilite la création et le partage d’applications web personnalisées pour l’analytique et la data science.

Nous allons commencer par développer notre première application web à partir de données Airbnb.

```
import streamlit as st
from snowflake.snowpark.context import get_active_session


st.title(f"AIRBNB Web App")
st.write(
  """This is our first Streamlit web app 
     application baseb on AIRBNB data!
  """
)

# Get connection
session = get_active_session()

# execute sql statement
sql = f"select count(*) as NB_LISTINGS, SENTIMENT_GENERATED from  AIRBNB.GOLD.FULL_MOON_REVIEWS_AUGMENTED group by SENTIMENT_GENERATED order by NB_LISTINGS asc;"

data = session.sql(sql).collect()

# Create a simple bar chart

st.subheader("Sentiment-Based Distribution of Listings")
st.bar_chart(data=data, x="SENTIMENT_GENERATED", y="NB_LISTINGS", color="SENTIMENT_GENERATED")

st.subheader("Underlying data")
st.dataframe(data, use_container_width=True)

```

![alt text](../images/img33.png)


* Ensuite, nous améliorerons le diagramme en barres en y ajoutant des filtres interactifs pour offrir une expérience utilisateur plus dynamique.

```
import streamlit as st
from snowflake.snowpark.context import get_active_session


st.title(f"AIRBNB Web App")
st.write(
  """This is our first Streamlit web app 
     application baseb on AIRBNB data!
  """
)

# Get connection
session = get_active_session()

# Create a select box
option = st.selectbox(
     'Select the Review Name?',
( 'Michael','Daniel','Thomas','David','Anna','Laura','Alexander','Julia','Maria','Martin','Andrea','Sarah','Christian','Lisa','Alex','Simon','Mark','Chris','Paul','Stefan','Nicole','Robert'))

# execute sql statement
sql = f"select count(*) as NB_LISTINGS, SENTIMENT_GENERATED from  AIRBNB.GOLD.FULL_MOON_REVIEWS_AUGMENTED  where reviewer_name= '{option}'group by SENTIMENT_GENERATED order by NB_LISTINGS asc;"

data = session.sql(sql).collect()

# Create a simple bar chart

st.subheader("Sentiment-Based Distribution of Listings")
st.bar_chart(data=data, x="SENTIMENT_GENERATED", y="NB_LISTINGS", color="SENTIMENT_GENERATED")

st.subheader("Underlying data")
st.dataframe(data, use_container_width=True)

```

![alt text](../images/img34.png)

## Réinitialisez votre compte Snowflake.
Exécutez les scripts ci-dessous pour rétablir votre compte dans l’état requis afin de relancer cet atelier.

```
USE ROLE ACCOUNTADMIN;

USE DATABASE AIRBNB;

USE SCHEMA SILVER;

DROP TABLE IF EXISTS HOSTS;

DROP TABLE IF EXISTS LISTINGS; 

DROP TABLE IF EXISTS REVIEWS; 

DROP TABLE IF EXISTS FULL_MOON_DATE; 

USE SCHEMA GOLD;

DROP VIEW IF EXISTS DIM_HOSTS;

DROP VIEW IF EXISTS DIML_ISTINGS; 

DROP TABLE IF EXISTS FACT_REVIEWS; 

DROP TABLE IF EXISTS FULL_MOON_REVIEWS;

DROP TABLE IF EXISTS FULL_MOON_REVIEWS_TRANSLATED;

DROP TABLE IF EXISTS FULL_MOON_REVIEWS_AUGMENTED;

DROP DATABASE IF EXISTS AIRBNB;

DROP WAREHOUSE IF EXISTS XLARGE_COMPUTE_WH;

```
