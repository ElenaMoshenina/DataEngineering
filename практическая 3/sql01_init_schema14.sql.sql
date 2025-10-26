-- Создание базы данных для анализа стоимости лекарств по диагнозам
-- Задание 14: Пациенты, Лекарства, Цены на лекарства - расчет общей стоимости назначенных лекарств для каждого диагноза

-- Таблица пациентов
CREATE TABLE IF NOT EXISTS patients (
    patient_id VARCHAR(10) PRIMARY KEY,
    age INTEGER NOT NULL CHECK (age >= 0 AND age <= 120),
    diagnosis VARCHAR(100) NOT NULL,
    gender VARCHAR(1) CHECK (gender IN ('М', 'Ж')),
    admission_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица назначенных лекарств
CREATE TABLE IF NOT EXISTS medications (
    prescription_id VARCHAR(10) PRIMARY KEY,
    patient_id VARCHAR(10) REFERENCES patients(patient_id),
    drug_name VARCHAR(100) NOT NULL,
    diagnosis VARCHAR(100) NOT NULL,
    dosage TEXT,
    duration_days INTEGER CHECK (duration_days > 0),
    prescribed_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица цен на лекарства
CREATE TABLE IF NOT EXISTS drug_prices (
    price_id SERIAL PRIMARY KEY,
    drug_name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) CHECK (price >= 0),
    currency VARCHAR(3) DEFAULT 'RUB',
    package_size INTEGER CHECK (package_size > 0),
    manufacturer VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_patients_diagnosis ON patients(diagnosis);
CREATE INDEX IF NOT EXISTS idx_medications_patient_id ON medications(patient_id);
CREATE INDEX IF NOT EXISTS idx_medications_diagnosis ON medications(diagnosis);
CREATE INDEX IF NOT EXISTS idx_medications_drug_name ON medications(drug_name);
CREATE INDEX IF NOT EXISTS idx_drug_prices_drug_name ON drug_prices(drug_name);

-- Представление для расчета стоимости лекарств по диагнозам
CREATE OR REPLACE VIEW diagnosis_cost_analysis AS
SELECT 
    p.diagnosis,
    COUNT(DISTINCT p.patient_id) AS patient_count,
    COUNT(m.prescription_id) AS total_prescriptions,
    COUNT(DISTINCT m.drug_name) AS unique_drugs_count,
    ROUND(AVG(dp.price), 2) AS avg_drug_price,
    SUM(dp.price) AS total_cost,
    ROUND(SUM(dp.price) / COUNT(DISTINCT p.patient_id), 2) AS cost_per_patient
FROM patients p
JOIN medications m ON p.patient_id = m.patient_id
JOIN drug_prices dp ON m.drug_name = dp.drug_name
GROUP BY p.diagnosis
ORDER BY total_cost DESC;

-- Представление для детального анализа по лекарствам
CREATE OR REPLACE VIEW drug_usage_analysis AS
SELECT 
    m.drug_name,
    dp.price,
    dp.manufacturer,
    COUNT(m.prescription_id) AS usage_count,
    COUNT(DISTINCT m.diagnosis) AS diagnoses_count,
    COUNT(DISTINCT m.patient_id) AS patients_count,
    ROUND(COUNT(m.prescription_id) * dp.price, 2) AS total_cost
FROM medications m
JOIN drug_prices dp ON m.drug_name = dp.drug_name
GROUP BY m.drug_name, dp.price, dp.manufacturer
ORDER BY total_cost DESC;