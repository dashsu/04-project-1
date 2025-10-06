CREATE TABLE Stock_Information (
  open           NUMERIC(12,2)    NOT NULL,
  high           NUMERIC(12,2)    NOT NULL,
  low            NUMERIC(12,2)    NOT NULL,
  close          NUMERIC(12,2)    NOT NULL,
  volume         BIGINT           NOT NULL,
  name           TEXT             NOT NULL,
  exchange_code  TEXT             ,
  asset_type     TEXT             NOT NULL,
  price_currency TEXT             NOT NULL,
  symbol         TEXT             NOT NULL,
  "date"         TIMESTAMPTZ      NOT NULL,
    etl_load_timestamp TIMESTAMPTZ DEFAULT now(),
  data_source      TEXT,
  PRIMARY KEY ("date", symbol)
);
