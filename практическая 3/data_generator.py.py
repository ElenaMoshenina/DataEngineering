"""
Генератор тестовых данных для задания 14: Анализ стоимости лекарств по диагнозам
Создает три файла:
1. patients.csv - данные о пациентах
2. medications.xlsx - данные о назначенных лекарствах
3. drug_prices.json - данные о ценах на лекарства
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
import random

# Настройка генератора случайных чисел для воспроизводимости
np.random.seed(42)
random.seed(42)

def generate_patients_data():
    """Генерация данных о пациентах (CSV)"""
    diagnoses = [
        "Гипертония", "Диабет 2 типа", "Бронхиальная астма", "Гастрит", 
        "Остеохондроз", "ОРВИ", "Грипп", "Пневмония", "Артрит", "Мигрень",
        "Аллергия", "Дерматит", "Гепатит", "Цистит", "Ангина"
    ]
    
    patients_data = []
    
    for i in range(1, 501):  # 500 пациентов
        patient = {
            "patient_id": f"P{i:04d}",
            "age": random.randint(18, 85),
            "diagnosis": random.choice(diagnoses),
            "gender": random.choice(["М", "Ж"]),
            "admission_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d")
        }
        patients_data.append(patient)
    
    df_patients = pd.DataFrame(patients_data)
    df_patients.to_csv('data/patients.csv', index=False, encoding='utf-8')
    print("✓ Файл patients.csv создан")
    return patients_data

def generate_medications_data(patients_data):
    """Генерация данных о назначенных лекарствах (Excel)"""
    common_drugs = [
        "Аспирин", "Парацетамол", "Ибупрофен", "Амоксициллин", "Метформин",
        "Лозартан", "Амлодипин", "Симвастатин", "Омепразол", "Сальбутамол",
        "Инсулин", "Левотироксин", "Аторвастатин", "Метопролол", "Варфарин",
        "Цетиризин", "Флуконазол", "Азитромицин", "Кларитромицин", "Диклофенак"
    ]
    
    # Создаем соответствие диагнозов и лекарств
    diagnosis_drugs = {
        "Гипертония": ["Лозартан", "Амлодипин", "Метопролол"],
        "Диабет 2 типа": ["Метформин", "Инсулин"],
        "Бронхиальная астма": ["Сальбутамол"],
        "Гастрит": ["Омепразол"],
        "Остеохондроз": ["Ибупрофен", "Диклофенак"],
        "ОРВИ": ["Парацетамол", "Аспирин"],
        "Грипп": ["Парацетамол", "Азитромицин"],
        "Пневмония": ["Амоксициллин", "Азитромицин"],
        "Артрит": ["Ибупрофен", "Диклофенак"],
        "Мигрень": ["Аспирин"],
        "Аллергия": ["Цетиризин"],
        "Дерматит": ["Флуконазол"],
        "Гепатит": ["Левотироксин"],
        "Цистит": ["Амоксициллин"],
        "Ангина": ["Азитромицин", "Кларитромицин"]
    }
    
    medications_data = []
    prescription_counter = 1
    
    for patient in patients_data:
        # Для каждого пациента назначаем от 1 до 4 лекарств
        num_medications = random.randint(1, 4)
        diagnosis = patient["diagnosis"]
        
        # Выбираем лекарства соответствующие диагнозу + возможно дополнительные
        diagnosis_specific_drugs = diagnosis_drugs.get(diagnosis, common_drugs)
        additional_drugs = random.sample([d for d in common_drugs if d not in diagnosis_specific_drugs], 
                                        min(2, len(common_drugs) - len(diagnosis_specific_drugs)))
        
        available_drugs = diagnosis_specific_drugs + additional_drugs
        
        for _ in range(num_medications):
            medication = {
                "prescription_id": f"RX{prescription_counter:05d}",
                "patient_id": patient["patient_id"],
                "drug_name": random.choice(available_drugs),
                "diagnosis": diagnosis,
                "dosage": f"{random.randint(1, 3)} таб. {random.randint(1, 3)} раза в день",
                "duration_days": random.randint(3, 30)
            }
            medications_data.append(medication)
            prescription_counter += 1
    
    df_medications = pd.DataFrame(medications_data)
    df_medications.to_excel('data/medications.xlsx', index=False)
    print("✓ Файл medications.xlsx создан")
    return medications_data

def generate_drug_prices_data(medications_data):
    """Генерация данных о ценах на лекарства (JSON)"""
    drugs = list(set([med['drug_name'] for med in medications_data]))
    
    prices_data = []
    
    for drug in drugs:
        # Базовая цена зависит от типа лекарства
        if drug in ["Инсулин", "Варфарин", "Левотироксин"]:
            base_price = random.uniform(500, 2000)
        elif drug in ["Аторвастатин", "Симвастатин", "Азитромицин"]:
            base_price = random.uniform(200, 800)
        else:
            base_price = random.uniform(50, 400)
        
        price_info = {
            "drug_name": drug,
            "price": round(base_price, 2),
            "currency": "RUB",
            "package_size": random.choice([10, 20, 30, 50, 100]),
            "manufacturer": random.choice(["ФармЗавод", "МедПродукт", "БиоФарм", "Фармакор", "Лекарства РФ"])
        }
        prices_data.append(price_info)
    
    # Сохраняем в JSON
    with open('data/drug_prices.json', 'w', encoding='utf-8') as f:
        json.dump(prices_data, f, ensure_ascii=False, indent=2)
    
    print("✓ Файл drug_prices.json создан")
    return prices_data

def main():
    """Основная функция генерации данных"""
    print("Генерация тестовых данных для анализа стоимости лекарств по диагнозам...")
    print("=" * 60)
    
    # Создаем папки если их нет
    for folder in ['data', 'results']:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"✓ Создана папка '{folder}'")
    
    # Генерируем данные
    patients_data = generate_patients_data()
    medications_data = generate_medications_data(patients_data)
    prices_data = generate_drug_prices_data(medications_data)
    
    print("=" * 60)
    print(f"Сгенерировано:")
    print(f"- Пациентов: {len(patients_data)}")
    print(f"- Назначений лекарств: {len(medications_data)}")
    print(f"- Записей о ценах на лекарства: {len(prices_data)}")
    print("\n✓ Все файлы сохранены в папке 'data/'")

if __name__ == "__main__":
    main()