{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Практическая работа 3: Анализ стоимости лекарств по диагнозам\n",
    "\n",
    "## Задание 14\n",
    "**Цель:** Рассчитать общую стоимость назначенных лекарств для каждого диагноза на основе консолидации данных из трех источников:\n",
    "- **Файл 1 (CSV):** Пациенты (patient_id, age, diagnosis)\n",
    "- **Файл 2 (Excel):** Лекарства (drug_name, diagnosis)\n",
    "- **Файл 3 (JSON):** Цены на лекарства (drug_name, price)\n",
    "\n",
    "## Библиотеки и подключения"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import numpy as np\n",
    "import matplotlib.pyplot as plt\n",
    "import seaborn as sns\n",
    "import plotly.express as px\n",
    "import plotly.graph_objects as go\n",
    "from plotly.subplots import make_subplots\n",
    "import json\n",
    "import warnings\n",
    "warnings.filterwarnings('ignore')\n",
    "\n",
    "# Настройка стиля графиков\n",
    "plt.style.use('seaborn-v0_8')\n",
    "sns.set_palette(\"husl\")\n",
    "plt.rcParams['figure.figsize'] = (12, 8)\n",
    "plt.rcParams['font.size'] = 12\n",
    "\n",
    "print(\"✓ Библиотеки загружены\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 1. Загрузка данных из различных источников"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Загрузка данных о пациентах (CSV)\n",
    "df_patients = pd.read_csv('data/patients.csv')\n",
    "print(\"Данные о пациентах (CSV):\")\n",
    "print(f\"Форма данных: {df_patients.shape}\")\n",
    "print(df_patients.head())\n",
    "print(\"\\n\" + \"=\"*50 + \"\\n\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Загрузка данных о назначенных лекарствах (Excel)\n",
    "df_medications = pd.read_excel('data/medications.xlsx')\n",
    "print(\"Данные о назначенных лекарствах (Excel):\")\n",
    "print(f\"Форма данных: {df_medications.shape}\")\n",
    "print(df_medications.head())\n",
    "print(\"\\n\" + \"=\"*50 + \"\\n\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Загрузка данных о ценах на лекарства (JSON)\n",
    "with open('data/drug_prices.json', 'r', encoding='utf-8') as f:\n",
    "    prices_data = json.load(f)\n",
    "\n",
    "df_prices = pd.DataFrame(prices_data)\n",
    "print(\"Данные о ценах на лекарства (JSON):\")\n",
    "print(f\"Форма данных: {df_prices.shape}\")\n",
    "print(df_prices.head())\n",
    "print(\"\\n\" + \"=\"*50 + \"\\n\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 2. Аудит и очистка данных"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Проверка данных о пациентах\n",
    "print(\"АУДИТ ДАННЫХ О ПАЦИЕНТАХ:\")\n",
    "print(\"=\" * 40)\n",
    "print(\"Информация о данных:\")\n",
    "print(df_patients.info())\n",
    "print(\"\\nПропущенные значения:\")\n",
    "print(df_patients.isnull().sum())\n",
    "print(\"\\nДубликаты пациентов:\")\n",
    "print(f\"Дубликаты patient_id: {df_patients['patient_id'].duplicated().sum()}\")\n",
    "print(\"\\nСтатистика по возрасту:\")\n",
    "print(df_patients['age'].describe())\n",
    "print(\"\\nРаспределение по диагнозам:\")\n",
    "print(df_patients['diagnosis'].value_counts().head(10))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Проверка данных о назначениях\n",
    "print(\"АУДИТ ДАННЫХ О НАЗНАЧЕННЫХ ЛЕКАРСТВАХ:\")\n",
    "print(\"=\" * 50)\n",
    "print(\"Пропущенные значения:\")\n",
    "print(df_medications.isnull().sum())\n",
    "print(\"\\nКоличество уникальных лекарств:\")\n",
    "print(f\"Уникальных лекарств: {df_medications['drug_name'].nunique()}\")\n",
    "print(\"\\nТоп-10 самых назначаемых лекарств:\")\n",
    "print(df_medications['drug_name'].value_counts().head(10))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Проверка данных о ценах\n",
    "print(\"АУДИТ ДАННЫХ О ЦЕНАХ НА ЛЕКАРСТВА:\")\n",
    "print(\"=\" * 45)\n",
    "print(\"Пропущенные значения:\")\n",
    "print(df_prices.isnull().sum())\n",
    "print(\"\\nСтатистика по ценам:\")\n",
    "print(df_prices['price'].describe())\n",
    "print(\"\\nСамые дорогие лекарства:\")\n",
    "print(df_prices.nlargest(5, 'price')[['drug_name', 'price', 'manufacturer']])"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 3. Консолидация данных и расчет стоимости"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Объединение всех данных\n",
    "# Шаг 1: Объединение назначений с ценами на лекарства\n",
    "df_meds_prices = pd.merge(df_medications, df_prices, on='drug_name', how='inner')\n",
    "\n",
    "# Шаг 2: Объединение с данными пациентов\n",
    "df_consolidated = pd.merge(df_meds_prices, df_patients, on=['patient_id', 'diagnosis'], how='inner')\n",
    "\n",
    "print(\"Консолидированные данные:\")\n",
    "print(f\"Форма данных: {df_consolidated.shape}\")\n",
    "print(df_consolidated[['patient_id', 'diagnosis', 'drug_name', 'price', 'age']].head())\n",
    "\n",
    "# Расчет общей стоимости для каждого диагноза\n",
    "diagnosis_costs = df_consolidated.groupby('diagnosis').agg({\n",
    "    'patient_id': 'nunique',\n",
    "    'prescription_id': 'count',\n",
    "    'drug_name': 'nunique',\n",
    "    'price': ['sum', 'mean']\n",
    "}).round(2)\n",
    "\n",
    "# Упрощение названий столбцов\n",
    "diagnosis_costs.columns = ['patient_count', 'total_prescriptions', 'unique_drugs', 'total_cost', 'avg_drug_price']\n",
    "diagnosis_costs['cost_per_patient'] = (diagnosis_costs['total_cost'] / diagnosis_costs['patient_count']).round(2)\n",
    "diagnosis_costs = diagnosis_costs.sort_values('total_cost', ascending=False)\n",
    "\n",
    "print(\"\\n\" + \"=\"*70)\n",
    "print(\"ОБЩАЯ СТОИМОСТЬ ЛЕКАРСТВ ПО ДИАГНОЗАМ\")\n",
    "print(\"=\"*70)\n",
    "print(diagnosis_costs)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 4. Детальный анализ по лекарствам"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Анализ использования лекарств\n",
    "drug_usage = df_consolidated.groupby('drug_name').agg({\n",
    "    'prescription_id': 'count',\n",
    "    'diagnosis': 'nunique',\n",
    "    'patient_id': 'nunique',\n",
    "    'price': 'mean'\n",
    "}).round(2)\n",
    "\n",
    "drug_usage.columns = ['usage_count', 'diagnoses_count', 'patients_count', 'avg_price']\n",
    "drug_usage['total_cost'] = (drug_usage['usage_count'] * drug_usage['avg_price']).round(2)\n",
    "drug_usage = drug_usage.sort_values('total_cost', ascending=False)\n",
    "\n",
    "print(\"АНАЛИЗ ИСПОЛЬЗОВАНИЯ ЛЕКАРСТВ:\")\n",
    "print(\"=\" * 50)\n",
    "print(drug_usage.head(10))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 5. Визуализация результатов"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# График 1: Общая стоимость лекарств по диагнозам (топ-10)\n",
    "top_diagnoses = diagnosis_costs.head(10)\n",
    "\n",
    "plt.figure(figsize=(14, 8))\n",
    "bars = plt.bar(range(len(top_diagnoses)), top_diagnoses['total_cost'], \n",
    "               color=sns.color_palette(\"viridis\", len(top_diagnoses)))\n",
    "plt.title('Общая стоимость лекарств по диагнозам (топ-10)', fontsize=16, fontweight='bold')\n",
    "plt.xlabel('Диагнозы', fontsize=12)\n",
    "plt.ylabel('Стоимость (руб.)', fontsize=12)\n",
    "plt.xticks(range(len(top_diagnoses)), top_diagnoses.index, rotation=45, ha='right')\n",
    "\n",
    "# Добавление значений на столбцы\n",
    "for i, bar in enumerate(bars):\n",
    "    height = bar.get_height()\n",
    "    plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,\n",
    "             f'{height:,.0f} руб.', ha='center', va='bottom', fontsize=10)\n",
    "\n",
    "plt.tight_layout()\n",
    "plt.grid(axis='y', alpha=0.3)\n",
    "plt.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# График 2: Стоимость на одного пациента по диагнозам\n",
    "plt.figure(figsize=(14, 8))\n",
    "cost_per_patient = diagnosis_costs.head(10).sort_values('cost_per_patient', ascending=True)\n",
    "\n",
    "bars = plt.barh(range(len(cost_per_patient)), cost_per_patient['cost_per_patient'],\n",
    "               color=sns.color_palette(\"plasma\", len(cost_per_patient)))\n",
    "plt.title('Средняя стоимость лечения на одного пациента по диагнозам', fontsize=16, fontweight='bold')\n",
    "plt.xlabel('Стоимость на пациента (руб.)', fontsize=12)\n",
    "plt.ylabel('Диагнозы', fontsize=12)\n",
    "plt.yticks(range(len(cost_per_patient)), cost_per_patient.index)\n",
    "\n",
    "# Добавление значений на столбцы\n",
    "for i, bar in enumerate(bars):\n",
    "    width = bar.get_width()\n",
    "    plt.text(width + width*0.01, bar.get_y() + bar.get_height()/2.,\n",
    "             f'{width:,.0f} руб.', ha='left', va='center', fontsize=10)\n",
    "\n",
    "plt.tight_layout()\n",
    "plt.grid(axis='x', alpha=0.3)\n",
    "plt.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# График 3: Круговая диаграмма распределения стоимости по диагнозам\n",
    "plt.figure(figsize=(12, 10))\n",
    "colors = plt.cm.Set3(np.linspace(0, 1, len(diagnosis_costs)))\n",
    "\n",
    "# Берем топ-8 диагнозов, остальные объединяем в \"Другие\"\n",
    "top_8 = diagnosis_costs.head(8)\n",
    "others = pd.DataFrame({\n",
    "    'total_cost': [diagnosis_costs['total_cost'][8:].sum()]\n",
    "}, index=['Другие'])\n",
    "\n",
    "pie_data = pd.concat([top_8[['total_cost']], others])\n",
    "\n",
    "wedges, texts, autotexts = plt.pie(pie_data['total_cost'], \n",
    "                                   labels=pie_data.index,\n",
    "                                   autopct='%1.1f%%', \n",
    "                                   colors=colors, \n",
    "                                   startangle=90,\n",
    "                                   textprops={'fontsize': 10})\n",
    "\n",
    "plt.title('Распределение общей стоимости лекарств по диагнозам', fontsize=16, fontweight='bold')\n",
    "\n",
    "# Улучшение читаемости\n",
    "for autotext in autotexts:\n",
    "    autotext.set_color('black')\n",
    "    autotext.set_fontweight('bold')\n",
    "\n",
    "plt.axis('equal')\n",
    "plt.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# График 4: Топ-10 самых дорогих лекарств по общей стоимости\n",
    "top_drugs = drug_usage.head(10)\n",
    "\n",
    "fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))\n",
    "\n",
    "# Левая диаграмма: общая стоимость\n",
    "bars1 = ax1.bar(range(len(top_drugs)), top_drugs['total_cost'], \n",
    "               color=sns.color_palette(\"coolwarm\", len(top_drugs)))\n",
    "ax1.set_title('Топ-10 лекарств по общей стоимости', fontsize=14, fontweight='bold')\n",
    "ax1.set_xlabel('Лекарства', fontsize=12)\n",
    "ax1.set_ylabel('Общая стоимость (руб.)', fontsize=12)\n",
    "ax1.set_xticks(range(len(top_drugs)))\n",
    "ax1.set_xticklabels(top_drugs.index, rotation=45, ha='right')\n",
    "ax1.grid(axis='y', alpha=0.3)\n",
    "\n",
    "# Правая диаграмма: количество назначений\n",
    "bars2 = ax2.bar(range(len(top_drugs)), top_drugs['usage_count'], \n",
    "               color=sns.color_palette(\"viridis\", len(top_drugs)))\n",
    "ax2.set_title('Топ-10 лекарств по количеству назначений', fontsize=14, fontweight='bold')\n",
    "ax2.set_xlabel('Лекарства', fontsize=12)\n",
    "ax2.set_ylabel('Количество назначений', fontsize=12)\n",
    "ax2.set_xticks(range(len(top_drugs)))\n",
    "ax2.set_xticklabels(top_drugs.index, rotation=45, ha='right')\n",
    "ax2.grid(axis='y', alpha=0.3)\n",
    "\n",
    "plt.tight_layout()\n",
    "plt.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 6. Интерактивная визуализация с Plotly"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Интерактивная столбчатая диаграмма\n",
    "fig = px.bar(diagnosis_costs.head(15).reset_index(), \n",
    "             x='diagnosis', \n",
    "             y='total_cost',\n",
    "             title='Общая стоимость лекарств по диагнозам (топ-15)',\n",
    "             labels={'diagnosis': 'Диагноз', 'total_cost': 'Общая стоимость (руб.)'},\n",
    "             color='total_cost',\n",
    "             color_continuous_scale='viridis')\n",
    "\n",
    "fig.update_layout(xaxis_tickangle=-45)\n",
    "fig.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Тепловая карта: стоимость vs количество пациентов\n",
    "heatmap_data = diagnosis_costs.head(15).reset_index()\n",
    "\n",
    "fig = px.scatter(heatmap_data, \n",
    "                 x='patient_count', \n",
    "                 y='total_cost',\n",
    "                 size='unique_drugs',\n",
    "                 color='cost_per_patient',\n",
    "                 hover_name='diagnosis',\n",
    "                 hover_data={'patient_count': True, 'total_cost': ':,', 'cost_per_patient': ':,'},\n",
    "                 title='Соотношение количества пациентов и стоимости лечения',\n",
    "                 labels={'patient_count': 'Количество пациентов', \n",
    "                         'total_cost': 'Общая стоимость (руб.)',\n",
    "                         'cost_per_patient': 'Стоимость на пациента (руб.)',\n",
    "                         'unique_drugs': 'Количество уникальных лекарств'},\n",
    "                 size_max=60)\n",
    "\n",
    "fig.update_traces(marker=dict(line=dict(width=1, color='DarkSlateGrey')))\n",
    "fig.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 7. Выводы и сохранение результатов"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Формирование выводов и сохранение результатов\n",
    "print(\"ОСНОВНЫЕ ВЫВОДЫ АНАЛИЗА СТОИМОСТИ ЛЕКАРСТВ\")\n",
    "print(\"=\" * 60)\n",
    "\n",
    "top_diagnosis = diagnosis_costs.index[0]\n",
    "top_cost = diagnosis_costs.iloc[0]['total_cost']\n",
    "top_cost_per_patient = diagnosis_costs.iloc[0]['cost_per_patient']\n",
    "total_medication_cost = diagnosis_costs['total_cost'].sum()\n",
    "\n",
    "print(f\"1. Самый затратный диагноз: {top_diagnosis}\")\n",
    "print(f\"   - Общая стоимость: {top_cost:,.0f} руб.\")\n",
    "print(f\"   - Стоимость на пациента: {top_cost_per_patient:,.0f} руб.\")\n",
    "print(f\"   - Количество пациентов: {diagnosis_costs.iloc[0]['patient_count']}\")\n",
    "\n",
    "print(f\"\\n2. Общая стоимость всех назначенных лекарств: {total_medication_cost:,.0f} руб.\")\n",
    "print(f\"3. Количество диагнозов в анализе: {len(diagnosis_costs)}\")\n",
    "print(f\"4. Средняя стоимость на пациента: {diagnosis_costs['cost_per_patient'].mean():,.0f} руб.\")\n",
    "\n",
    "# Топ-3 самых дорогих диагнозов\n",
    "print(\"\\n5. ТОП-3 самых затратных диагнозов:\")\n",
    "for i, (diagnosis, row) in enumerate(diagnosis_costs.head(3).iterrows(), 1):\n",
    "    cost = row['total_cost']\n",
    "    patients = row['patient_count']\n",
    "    cost_per_pat = row['cost_per_patient']\n",
    "    print(f\"   {i}. {diagnosis}: {cost:,.0f} руб. ({patients} пациентов, {cost_per_pat:,.0f} руб./пациент)\")\n",
    "\n",
    "# Сохранение результатов в Excel\n",
    "with pd.ExcelWriter('results/diagnosis_cost_analysis.xlsx') as writer:\n",
    "    diagnosis_costs.to_excel(writer, sheet_name='Стоимость по диагнозам')\n",
    "    drug_usage.to_excel(writer, sheet_name='Использование лекарств')\n",
    "    df_consolidated.to_excel(writer, sheet_name='Консолидированные данные', index=False)\n",
    "\n",
    "print(f\"\\n✓ Результаты сохранены в файл: 'results/diagnosis_cost_analysis.xlsx'\")\n",
    "\n",
    "# Сохранение сводной статистики\n",
    "summary_stats = {\n",
    "    'total_patients': df_patients['patient_id'].nunique(),\n",
    "    'total_prescriptions': len(df_medications),\n",
    "    'total_drugs': df_prices['drug_name'].nunique(),\n",
    "    'total_diagnoses': df_patients['diagnosis'].nunique(),\n",
    "    'total_medication_cost': total_medication_cost,\n",
    "    'avg_cost_per_patient': diagnosis_costs['cost_per_patient'].mean(),\n",
    "    'most_expensive_diagnosis': top_diagnosis,\n",
    "    'most_used_drug': drug_usage.index[0]\n",
    "}\n",
    "\n",
    "with open('results/summary_statistics.json', 'w', encoding='utf-8') as f:\n",
    "    json.dump(summary_stats, f, ensure_ascii=False, indent=2)\n",
    "\n",
    "print(f\"✓ Сводная статистика сохранена в файл: 'results/summary_statistics.json'\")"
   ]
  }
 ],
 "metadata": {\n",
  "kernelspec": {\n",
  "display_name": "Python 3\",\n",
  "language\": \"python\",\n",
  "name\": \"python3\"\n",
  },\n",
  "language_info\": {\n",
  "codemirror_mode\": {\n",
  "name\": \"ipython\",\n",
  "version\": 3\n",
  },\n",
  "file_extension\": \".py\",\n",
  "mimetype\": \"text/x-python\",\n",
  "name\": \"python\",\n",
  "nbconvert_exporter\": \"python\",\n",
  "pygments_lexer\": \"ipython3\",\n",
  "version\": \"3.8.5\"\n",
  }\n",
  },\n",
  "nbformat\": 4,\n",
  "nbformat_minor\": 4\n",
  "}"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}