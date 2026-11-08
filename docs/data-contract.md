# Data contract: landing files

This is the agreement between the fund administrators who send files and this pipeline.
Every layout is mapped to the canonical columns below, so the rest of the pipeline never
depends on how one administrator names its columns.

The grain of both files is the **installment** (*parcela*). A contract has one or more
installments, and each installment is identified by its own number, the *nosso número*.

## Keys

| File | Grain | Unique key | Scope |
|---|---|---|---|
| Acquisitions | one installment | `installment_id` | per fund |
| Settlements | one payment event | `transaction_id` | per fund |

A settlement points to the installment it pays through `installment_id`. That link is how a
settlement writes an installment off the portfolio (*dar baixa no estoque*). An installment
can have several settlement rows, for example a partial payment followed by the final one.

## Acquisitions file

| Canonical column | Example source column | Type | Notes |
|---|---|---|---|
| `contract_id` | `NU_CONTRATO` | string | Groups the installments of one contract |
| `installment_id` | `NOSSO_NUMERO` | string | Unique key, required |
| `installment_number` | `NUMERO_PARCELA` | integer | 1 or more |
| `receivable_type` | | string | `TRADE_BILL`, `SERVICE_BILL`, `CREDIT_NOTE` or `PROMISSORY_NOTE` |
| `seller_name`, `seller_doc` | | string | CPF/CNPJ with valid check digits |
| `originator_name`, `originator_doc` | | string | |
| `debtor_name`, `debtor_doc` | | string | CPF/CNPJ with valid check digits |
| `issue_date` | | date | |
| `due_date` | | date | Per installment |
| `acquisition_date` | | date | |
| `face_value` | | decimal | Per installment |
| `purchase_price` | | decimal | Per installment |

## Settlements file

| Canonical column | Example source column | Type | Notes |
|---|---|---|---|
| `installment_id` | `NOSSO_NUMERO` | string | Foreign key to the acquisition, required |
| `transaction_id` | provider column, or derived | string | Unique key, see below |
| `settlement_date` | | date | |
| `settlement_type` | | string | `FULL`, `PARTIAL` or `CANCELLATION` |
| `amount_paid` | | decimal | Zero or more |

### Transaction ID

A layout declares one of two modes:

- **Provider column:** the file carries its own payment identifier. It is required, and
  repeating it with different contents is a conflict.
- **Derived:** the file has none, so the pipeline builds one as a hash of
  `fund_code`, `installment_id`, `settlement_date`, `settlement_type` and `amount_paid`.
  The fields are trimmed and joined with `|` in that order. The amount is written with
  exactly two decimals, parsed as an exact decimal (never a float) and rounded half up,
  so `10,555` and `10,56` give the same ID in every engine.
  The same payment always gets the same ID, whatever file or position it arrives in, so a
  corrected file keeps the IDs of the rows that did not change.

**Known limit of derived IDs:** two real payments with the same installment, date, type and
amount have the same hash and cannot be told apart from an accidental duplicate. They are
treated as exact copies and one row is kept. A provider column removes this limit.

## Validation rules

The first failing rule, in the order listed, is the single reason recorded for a row.

Acquisitions:
1. `missing_installment_id`
2. `invalid_installment_number`
3. `invalid_debtor_doc`
4. `invalid_seller_doc`
5. `invalid_date`
6. `non_positive_face_value`
7. `purchase_price_out_of_range`
8. `due_before_acquisition`
9. `conflicting_duplicate_in_file`: the same `installment_id` with different contents
10. `duplicate_in_file`: an exact copy of an earlier row

Settlements:
1. `missing_installment_id`
2. `missing_transaction_id` (only for layouts with a provider column)
3. `invalid_date`
4. `unknown_settlement_type`
5. `negative_amount`
6. `conflicting_duplicate_in_file`: the same `transaction_id` with different contents
7. `duplicate_in_file`: an exact copy of an earlier row

A missing or unreadable value counts as a failure of the rule that needs it, never as a pass.

## File versions and idempotency

- A file is fingerprinted with SHA-256. Receiving the same bytes again loads nothing.
- A different file with the same name is a corrected version. Only the latest version of
  each file is used downstream.
- Row position in a file is kept for tracing rejected rows. It is not part of any key.
  A parallel reader has no line numbers, so the position orders the rows of a file by their
  original content. It identifies a row within a file version, but it is not the line.
- `file_id` is derived from the file's SHA-256, so the same bytes get the same ID in every
  environment. It says nothing about age: the order of versions comes from `ingested_at`.

## Layers

| Layer | Schema (local / Databricks) | Content |
|---|---|---|
| Bronze | Parquet, `fidc_bronze` | Every row as text, with the file it came from |
| Silver | `silver` / `fidc_silver` | Accepted rows, typed |
| Quarantine | `meta` / `fidc_meta` | `rejected_rows` and `file_ledger` |
| Gold | `marts` / `fidc_marts` | dbt models |
