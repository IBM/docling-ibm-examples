# Prompts

Send these to IBM Bob one at a time, in the order below. Read what Bob does (which
commands it runs, which sections it reads), not only what it answers.

## 1. Convert and explore one document

```text
Convert docs/vantara_risk_disclosure.pdf to Markdown and DocLang.
```

The first time, Bob asks to use the skill. Choose **Approve skill tools for task**.

```text
Use dclq to show me the outline of the risk disclosure, with the XPath of each section.
```

```text
Find where the document talks about risk exposure, and show me only that section.
```

```text
What are the main risk categories in this document? Which one is the largest, and in which table did you find it?
```

## 2. Fill the form from three documents

```text
Convert all documents in docs/ to Markdown and DocLang.
```

```text
Use dclq to show the outline of the audit summary. Which section lists the material findings, and which one has the sign-off?
```

```text
Fill docs/compliance_assessment_form.json using the three converted documents.
For each field, use dclq to find the section that contains it, and read only that section.
Return the completed JSON, and after it a table with one row per field showing
the source document and the section XPath you took it from.
Save the result as converted/compliance_assessment_form_filled.json.
```

## 3. Cross-check

```text
The total risk exposure and the regulator reference number appear in two documents.
Compare the values from each. Do they agree?
```

## 4. Write the report

```text
Write a three-sentence executive summary covering the total risk exposure,
the highest risk category and the most important obligations.
Then write converted/compliance_report.md with the summary followed by
the filled form as a Markdown table, including the source column.
```

> **Check the numbers.** The field values come from Bob's language model reading the
> converted documents. Docling for IBM watsonx supplies the structure (sections, tables,
> cells), not the values. Compare each value with the section Bob cited, especially
> amounts, units and dates.
