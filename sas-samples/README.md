# Exemples SAS

Les deux fichiers contiennent les mêmes 32 observations et 19 colonnes synthétiques.

- all_types.xport : SAS XPORT V5, encodage UTF-8 (à sélectionner explicitement à la lecture).
- all_types.sas7bdat : SAS7BDAT, encodage UTF-8 déclaré dans les métadonnées.
- preview.csv : aperçu des valeurs brutes (dates et heures sous forme numérique SAS).

SAS possède deux types de stockage : caractère et numérique. Les entiers et booléens sont numériques ; BOOLEAN utilise 0/1. Les dates, dates-heures et heures ont leur format SAS associé. Les montants et pourcentages sont des nombres en virgule flottante, sans stockage décimal exact.

None et NaN deviennent la valeur numérique manquante SAS standard (.). La colonne SPECIAL contient les 28 codes manquants : ._, ., .A à .Z, puis des valeurs ordinaires. Les chaînes vides, absentes ou composées uniquement de blancs ne sont pas distinguables en SAS. Les textes littéraux NULL et NaN restent des chaînes.

MISSNUM et MISSTXT sont entièrement vides. La ligne ID=31 ne contient aucune autre valeur renseignée. Les exemples couvrent aussi les accents, les retours à la ligne, les codes avec zéros initiaux, les grands entiers exacts, la notation scientifique, les valeurs négatives, les dates avant 1960 et les secondes fractionnaires.

Validation : relecture des deux fichiers avec Pandas 2.2.3 et comparaison aux valeurs sources, formats des dates vérifiés, contrôle des 28 codes manquants dans chaque fichier avec ReadStat. Aucun moteur SAS natif disponible pour vérifier leur ouverture dans SAS. Le générateur utilise ReadStat : https://github.com/WizardMac/ReadStat ; détails de cette limite : https://github.com/WizardMac/ReadStat#readstat-read-and-write-data-sets-from-sas-stata-and-spss

| Colonne | Contenu | Format SAS |
| --- | --- | --- |
| ID | Observation identifier | BEST12. |
| TEXT | Text, accents, punctuation, empty | caractère |
| LONGTXT | Long character values | caractère |
| CODE | String codes preserving leading zeros | caractère |
| INTEGER | Integer-valued SAS numeric | BEST12. |
| BIGINT | Exact integers up to 2**53 minus 1 | BEST32. |
| DOUBLE | Double precision numeric | BEST32. |
| SCIENTIF | Small and large scientific values | E24.16 |
| BOOLEAN | Boolean convention: 0 false, 1 true | 1. |
| DATEVAL | Date: days since 1960-01-01 | DATE9. |
| DATEISO | Same date displayed in ISO format | E8601DA10. |
| DATETIME | Datetime: seconds since 1960-01-01 | DATETIME26.6 |
| TIMEVAL | Time of day: seconds since midnight | TIME15.6 |
| MONEY | Numeric with a currency format | DOLLAR16.2 |
| PERCENT | Numeric with a percent format | PERCENT12.2 |
| MISSNUM | Entirely missing numeric column | BEST12. |
| MISSTXT | Entirely blank character column | caractère |
| SPECIAL | All 28 SAS numeric missing codes | BEST12. |
| CASE | Description of the observation | caractère |
