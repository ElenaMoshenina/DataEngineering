"""
Основной скрипт для анализа стоимости лекарств по диагнозам
Задание 14: Расчет общей стоимости назначенных лекарств для каждого диагноза
"""

import pandas as pd
import numpy as np
import json
import os
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

class MedicationCostAnalyzer:
    """Класс для анализа стоимости лекарств по диагнозам"""
    
    def __init__(self):
        self.df_patients = None
        self.df_medications = None
        self.df_prices = None
        self.df_consolidated = None
        self.diagnosis_costs = None
        self.drug_usage = None
        
    def load_data(self):
        """Загрузка данных из различных источников"""
        print("Загрузка данных...")
        
        try:
            # Загрузка данных о пациентах (CSV)
            self.df_patients = pd.read_csv('data/patients.csv')
            print(f"✓ Загружены данные о {len(self.df_patients)} пациентах")
            
            # Загрузка данных о назначениях (Excel)
            self.df_medications = pd.read_excel('data/medications.xlsx')
            print(f"✓ Загружены данные о {len(self.df_medications)} назначениях")
            
            # Загрузка данных о ценах (JSON)
            with open('data/drug_prices.json', 'r', encoding='utf-8') as f:
                prices_data = json.load(f)
            self.df_prices = pd.DataFrame(prices_data)
            print(f"✓ Загружены данные о {len(self.df_prices)} лекарствах")
            
            return True
            
        except FileNotFoundError as e:
            print(f"❌ Ошибка загрузки данных: {e}")
            print("Запустите сначала data_generator.py для создания тестовых данных")
            return False
        except Exception as e:
            print(f"❌ Неожиданная ошибка при загрузке данных: {e}")
            return False
    
    def data_quality_check(self):
        """Проверка качества данных"""
        print("\nПроверка качества данных...")
        
        try:
            # Проверка пропущенных значений
            print("Пропущенные значения:")
            print(f"  - Пациенты: {self.df_patients.isnull().sum().sum()}")
            print(f"  - Назначения: {self.df_medications.isnull().sum().sum()}")
            print(f"  - Цены: {self.df_prices.isnull().sum().sum()}")
            
            # Проверка дубликатов
            print("\nДубликаты:")
            print(f"  - Пациенты: {self.df_patients['patient_id'].duplicated().sum()}")
            print(f"  - Назначения: {self.df_medications['prescription_id'].duplicated().sum()}")
            print(f"  - Цены: {self.df_prices.duplicated(subset=['drug_name']).sum()}")
            
            # Базовая статистика
            print("\nБазовая статистика:")
            print(f"  - Уникальных диагнозов: {self.df_patients['diagnosis'].nunique()}")
            print(f"  - Уникальных лекарств: {self.df_medications['drug_name'].nunique()}")
            print(f"  - Средний возраст пациентов: {self.df_patients['age'].mean():.1f} лет")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка при проверке качества данных: {e}")
            return False
    
    def consolidate_data(self):
        """Консолидация данных из трех источников"""
        print("\nКонсолидация данных...")
        
        try:
            # Объединение назначений с ценами
            df_meds_prices = pd.merge(self.df_medications, self.df_prices, 
                                     on='drug_name', how='inner')
            
            # Объединение с данными пациентов
            self.df_consolidated = pd.merge(df_meds_prices, self.df_patients, 
                                           on=['patient_id', 'diagnosis'], how='inner')
            
            print(f"✓ Консолидированные данные: {len(self.df_consolidated)} записей")
            
            # Расчет стоимости по диагнозам
            self.calculate_diagnosis_costs()
            
            # Анализ использования лекарств
            self.analyze_drug_usage()
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка при консолидации данных: {e}")
            return False
    
    def calculate_diagnosis_costs(self):
        """Расчет общей стоимости лекарств для каждого диагноза"""
        try:
            # Группировка по диагнозам
            diagnosis_groups = self.df_consolidated.groupby('diagnosis')
            
            # Агрегация данных
            self.diagnosis_costs = diagnosis_groups.agg({
                'patient_id': 'nunique',
                'prescription_id': 'count',
                'drug_name': 'nunique',
                'price': 'sum'
            }).round(2)
            
            # Переименование столбцов
            self.diagnosis_costs.columns = ['patient_count', 'total_prescriptions', 'unique_drugs', 'total_cost']
            
            # Расчет дополнительных метрик
            self.diagnosis_costs['avg_cost_per_patient'] = (
                self.diagnosis_costs['total_cost'] / self.diagnosis_costs['patient_count']
            ).round(2)
            
            self.diagnosis_costs['avg_prescriptions_per_patient'] = (
                self.diagnosis_costs['total_prescriptions'] / self.diagnosis_costs['patient_count']
            ).round(2)
            
            # Сортировка по общей стоимости
            self.diagnosis_costs = self.diagnosis_costs.sort_values('total_cost', ascending=False)
            
            print(f"✓ Рассчитана стоимость для {len(self.diagnosis_costs)} диагнозов")
            
        except Exception as e:
            print(f"❌ Ошибка при расчете стоимости по диагнозам: {e}")
            raise
    
    def analyze_drug_usage(self):
        """Анализ использования лекарств"""
        try:
            # Группировка по лекарствам
            drug_groups = self.df_consolidated.groupby('drug_name')
            
            self.drug_usage = drug_groups.agg({
                'prescription_id': 'count',
                'diagnosis': 'nunique',
                'patient_id': 'nunique',
                'price': 'mean'
            }).round(2)
            
            # Переименование столбцов
            self.drug_usage.columns = ['usage_count', 'diagnoses_count', 'patients_count', 'avg_price']
            
            # Расчет общей стоимости по лекарству
            self.drug_usage['total_cost'] = (
                self.drug_usage['usage_count'] * self.drug_usage['avg_price']
            ).round(2)
            
            # Сортировка по общей стоимости
            self.drug_usage = self.drug_usage.sort_values('total_cost', ascending=False)
            
            print(f"✓ Проанализировано {len(self.drug_usage)} лекарств")
            
        except Exception as e:
            print(f"❌ Ошибка при анализе использования лекарств: {e}")
            raise
    
    def generate_report(self):
        """Генерация отчета с основными выводами"""
        try:
            print("\n" + "="*70)
            print("АНАЛИТИЧЕСКИЙ ОТЧЕТ: СТОИМОСТЬ ЛЕКАРСТВ ПО ДИАГНОЗАМ")
            print("="*70)
            
            total_cost = self.diagnosis_costs['total_cost'].sum()
            
            print(f"\n📊 ОБЩАЯ СТАТИСТИКА:")
            print(f"  • Общая стоимость всех лекарств: {total_cost:,.0f} руб.")
            print(f"  • Количество диагнозов: {len(self.diagnosis_costs)}")
            print(f"  • Количество пациентов: {self.df_patients['patient_id'].nunique()}")
            print(f"  • Количество назначений: {len(self.df_medications)}")
            print(f"  • Количество уникальных лекарств: {self.df_prices['drug_name'].nunique()}")
            
            print(f"\n🏥 ТОП-5 САМЫХ ЗАТРАТНЫХ ДИАГНОЗОВ:")
            for i, (diagnosis, row) in enumerate(self.diagnosis_costs.head(5).iterrows(), 1):
                cost = row['total_cost']
                patients = int(row['patient_count'])
                cost_per_patient = row['avg_cost_per_patient']
                percentage = (cost / total_cost) * 100
                
                print(f"  {i}. {diagnosis}:")
                print(f"     • Общая стоимость: {cost:,.0f} руб. ({percentage:.1f}%)")
                print(f"     • Пациентов: {patients}")
                print(f"     • Стоимость на пациента: {cost_per_patient:,.0f} руб.")
                print(f"     • Назначений: {int(row['total_prescriptions'])}")
            
            print(f"\n💊 ТОП-5 САМЫХ ИСПОЛЬЗУЕМЫХ ЛЕКАРСТВ:")
            for i, (drug, row) in enumerate(self.drug_usage.head(5).iterrows(), 1):
                usage = int(row['usage_count'])
                drug_cost = row['total_cost']
                avg_price = row['avg_price']
                patients = int(row['patients_count'])
                
                print(f"  {i}. {drug}:")
                print(f"     • Назначений: {usage}")
                print(f"     • Общая стоимость: {drug_cost:,.0f} руб.")
                print(f"     • Средняя цена: {avg_price:,.0f} руб.")
                print(f"     • Пациентов: {patients}")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка при генерации отчета: {e}")
            return False
    
    def create_visualizations(self):
        """Создание визуализаций"""
        try:
            print("\nСоздание визуализаций...")
            
            # Настройка стиля графиков
            plt.style.use('default')
            sns.set_palette("husl")
            plt.rcParams['figure.figsize'] = (12, 8)
            plt.rcParams['font.size'] = 12
            
            # График 1: Общая стоимость по диагнозам (топ-10)
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
            
            top_10_diagnoses = self.diagnosis_costs.head(10)
            
            # Левая диаграмма: общая стоимость
            bars1 = ax1.bar(range(len(top_10_diagnoses)), top_10_diagnoses['total_cost'], 
                          color=sns.color_palette("viridis", len(top_10_diagnoses)))
            ax1.set_title('Общая стоимость лекарств по диагнозам (топ-10)', fontsize=14, fontweight='bold')
            ax1.set_xlabel('Диагнозы', fontsize=12)
            ax1.set_ylabel('Стоимость (руб.)', fontsize=12)
            ax1.set_xticks(range(len(top_10_diagnoses)))
            ax1.set_xticklabels(top_10_diagnoses.index, rotation=45, ha='right')
            ax1.grid(axis='y', alpha=0.3)
            
            # Добавление значений на столбцы
            for i, bar in enumerate(bars1):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                        f'{height:,.0f}', ha='center', va='bottom', fontsize=9)
            
            # Правая диаграмма: стоимость на пациента
            cost_per_patient = top_10_diagnoses.sort_values('avg_cost_per_patient', ascending=True)
            bars2 = ax2.barh(range(len(cost_per_patient)), cost_per_patient['avg_cost_per_patient'],
                           color=sns.color_palette("plasma", len(cost_per_patient)))
            ax2.set_title('Средняя стоимость на пациента по диагнозам', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Стоимость на пациента (руб.)', fontsize=12)
            ax2.set_ylabel('Диагнозы', fontsize=12)
            ax2.set_yticks(range(len(cost_per_patient)))
            ax2.set_yticklabels(cost_per_patient.index)
            ax2.grid(axis='x', alpha=0.3)
            
            # Добавление значений на столбцы
            for i, bar in enumerate(bars2):
                width = bar.get_width()
                ax2.text(width + width*0.01, bar.get_y() + bar.get_height()/2.,
                        f'{width:,.0f}', ha='left', va='center', fontsize=9)
            
            plt.tight_layout()
            plt.savefig('results/diagnosis_costs_comparison.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            # График 2: Круговая диаграмма распределения стоимости
            plt.figure(figsize=(10, 8))
            
            # Берем топ-6 диагнозов, остальные объединяем в "Другие"
            top_6 = self.diagnosis_costs.head(6)
            others_cost = self.diagnosis_costs['total_cost'][6:].sum()
            
            pie_data = top_6['total_cost'].tolist() + [others_cost]
            pie_labels = top_6.index.tolist() + ['Другие']
            
            colors = plt.cm.Set3(np.linspace(0, 1, len(pie_data)))
            wedges, texts, autotexts = plt.pie(pie_data, labels=pie_labels, autopct='%1.1f%%',
                                              colors=colors, startangle=90)
            
            plt.title('Распределение общей стоимости лекарств по диагнозам', 
                     fontsize=16, fontweight='bold', pad=20)
            
            # Улучшение читаемости
            for autotext in autotexts:
                autotext.set_color('black')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(10)
            
            plt.axis('equal')
            plt.savefig('results/cost_distribution_pie.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            print("✓ Визуализации сохранены в папке 'results/'")
            print("  - diagnosis_costs_comparison.png")
            print("  - cost_distribution_pie.png")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка при создании визуализаций: {e}")
            return False
    
    def save_results(self):
        """Сохранение результатов анализа"""
        try:
            print("\nСохранение результатов...")
            
            # Сохранение в Excel
            with pd.ExcelWriter('results/diagnosis_cost_analysis.xlsx') as writer:
                self.diagnosis_costs.to_excel(writer, sheet_name='Стоимость по диагнозам')
                self.drug_usage.to_excel(writer, sheet_name='Использование лекарств')
                self.df_consolidated.to_excel(writer, sheet_name='Консолидированные данные', index=False)
            
            # Сохранение сводной статистики
            summary_stats = {
                'analysis_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'total_patients': int(self.df_patients['patient_id'].nunique()),
                'total_prescriptions': int(len(self.df_medications)),
                'total_drugs': int(self.df_prices['drug_name'].nunique()),
                'total_diagnoses': int(self.df_patients['diagnosis'].nunique()),
                'total_medication_cost': float(self.diagnosis_costs['total_cost'].sum()),
                'avg_cost_per_patient': float(self.diagnosis_costs['avg_cost_per_patient'].mean()),
                'most_expensive_diagnosis': self.diagnosis_costs.index[0],
                'most_used_drug': self.drug_usage.index[0],
                'top_diagnoses': {}
            }
            
            # Добавляем топ-5 диагнозов
            for diagnosis, row in self.diagnosis_costs.head(5).iterrows():
                summary_stats['top_diagnoses'][diagnosis] = {
                    'total_cost': float(row['total_cost']),
                    'patient_count': int(row['patient_count']),
                    'avg_cost_per_patient': float(row['avg_cost_per_patient'])
                }
            
            with open('results/summary_statistics.json', 'w', encoding='utf-8') as f:
                json.dump(summary_stats, f, ensure_ascii=False, indent=2)
            
            print("✓ Результаты сохранены в папке 'results/'")
            print("  - diagnosis_cost_analysis.xlsx")
            print("  - summary_statistics.json")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка при сохранении результатов: {e}")
            return False
    
    def run_analysis(self):
        """Запуск полного анализа"""
        print("Запуск анализа стоимости лекарств по диагнозам...")
        print("=" * 60)
        
        # Создаем папку results если её нет
        if not os.path.exists('results'):
            os.makedirs('results')
        
        # Последовательное выполнение этапов анализа
        steps = [
            ("Загрузка данных", self.load_data),
            ("Проверка качества данных", self.data_quality_check),
            ("Консолидация данных", self.consolidate_data),
            ("Генерация отчета", self.generate_report),
            ("Создание визуализаций", self.create_visualizations),
            ("Сохранение результатов", self.save_results)
        ]
        
        for step_name, step_func in steps:
            print(f"\n[{step_name}]")
            success = step_func()
            if not success:
                print(f"❌ Ошибка на этапе: {step_name}")
                return False
        
        print("\n" + "="*60)
        print("🎉 Анализ успешно завершен!")
        print("\n📋 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
        print(f"   • Общая стоимость лекарств: {self.diagnosis_costs['total_cost'].sum():,.0f} руб.")
        print(f"   • Самый затратный диагноз: {self.diagnosis_costs.index[0]}")
        print(f"   • Самое используемое лекарство: {self.drug_usage.index[0]}")
        print(f"   • Все результаты сохранены в папке 'results/'")
        
        return True

def main():
    """Основная функция"""
    analyzer = MedicationCostAnalyzer()
    success = analyzer.run_analysis()
    
    if success:
        print("\n✅ Анализ выполнен успешно!")
        return 0
    else:
        print("\n❌ Анализ завершился с ошибками")
        return 1

if __name__ == "__main__":
    exit(main())