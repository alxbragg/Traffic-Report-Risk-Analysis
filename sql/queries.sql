-- 1.
-- Standardize vehicle makes via normalization, junk filtering, manual fixes,
-- frequency-based trust, and conservative matching with reasoned confidence.

WITH make_counts AS (
    SELECT
        upper(trim(make)) AS make_raw,
        count(*)::bigint AS count
    FROM traffic_violations
    GROUP BY upper(trim(make))
),
normalized AS (
    SELECT
        make_raw,
        count,
        -- 1) Normalize: upper/trim already; collapse whitespace to single spaces
        regexp_replace(make_raw, '\s+', ' ', 'g') AS make_clean
    FROM make_counts
),
junk AS (
    SELECT
        make_raw,
        count,
        make_clean,

        -- 2) Detect junk
        (make_clean ~ '\d') AS has_digits,
        NOT (make_clean ~ '^[A-Z &/\-]+$') AS bad_chars,
        (coalesce(length(make_clean), 0) < 2) AS too_short,
        (coalesce(length(make_clean), 0) > 25) AS too_long,

        CASE
            WHEN make_clean ~ '\d' THEN 'digits_present'
            WHEN NOT (make_clean ~ '^[A-Z &/\-]+$') THEN 'invalid_chars'
            WHEN coalesce(length(make_clean), 0) < 2 THEN 'too_short'
            WHEN coalesce(length(make_clean), 0) > 25 THEN 'too_long'
            ELSE 'ok'
        END AS junk_reason
    FROM normalized
),
stage1 AS (
    SELECT
        make_raw,
        count,
        make_clean,
        has_digits, bad_chars, too_short, too_long,
        junk_reason,
        CASE WHEN junk_reason = 'ok' THEN make_clean ELSE NULL END AS make_stage1
    FROM junk
),
stage2 AS (
    SELECT
        make_raw,
        count,
        make_clean,
        junk_reason,
        make_stage1,

        -- 3) Manual fixes (high-certainty)
        CASE make_stage1
            WHEN 'TOYT' THEN 'TOYOTA'
            WHEN 'TOYO' THEN 'TOYOTA'
            WHEN 'TYOTA' THEN 'TOYOTA'
            WHEN 'CHEV' THEN 'CHEVROLET'
            WHEN 'CHEVY' THEN 'CHEVROLET'
            WHEN 'MERCADES BENZ' THEN 'MERCEDES'
            WHEN 'MERCEDES BENZ' THEN 'MERCEDES'
            WHEN 'MITSHUBISHU' THEN 'MITSUBISHI'
            WHEN 'VOLKS' THEN 'VOLKSWAGEN'
            WHEN 'VW' THEN 'VOLKSWAGEN'
            ELSE make_stage1
        END AS make_stage2
    FROM stage1
),
trusted AS (
    -- 4) Trusted list from frequency threshold
    SELECT DISTINCT make_stage2 AS trusted_make
    FROM stage2
    WHERE count >= 500
      AND make_stage2 IS NOT NULL
)
SELECT
    s.make_raw,
    s.count,
    s.make_clean,
    s.junk_reason,
    s.make_stage2,

    -- "standardize" without fuzzy:
    CASE
        WHEN s.make_stage2 IS NULL THEN NULL
        WHEN s.make_stage2 IN (SELECT trusted_make FROM trusted)
            THEN s.make_stage2
        ELSE s.make_stage2
    END AS make_standard,

    CASE
        WHEN s.make_stage2 IS NULL THEN 'filtered'
        WHEN s.make_stage2 IN (SELECT trusted_make FROM trusted) THEN 'trusted'
        ELSE 'no_fuzzy'
    END AS make_reason,

    CASE
        WHEN s.make_stage2 IS NULL THEN 'low'
        WHEN s.make_stage2 IN (SELECT trusted_make FROM trusted) THEN 'high'
        ELSE 'low'
    END AS make_confidence
FROM stage2 s
ORDER BY s.count DESC, s.make_raw;

-- 2.
-- Map the car makes more accurately
SELECT
    *,
    CASE upper(trim(make))

        WHEN 'ACUR' THEN 'ACURA'

        WHEN 'CADI' THEN 'CADILLAC'

        WHEN 'CHRY' THEN 'CHRYSLER'

        WHEN 'DODG' THEN 'DODGE'

        WHEN 'HOND' THEN 'HONDA'

        WHEN 'HYUN' THEN 'HYUNDAI'

        WHEN 'JAG' THEN 'JAGUAR'

        WHEN 'MAZD' THEN 'MAZDA'

        WHEN 'MITS' THEN 'MITSUBISHI'

        WHEN 'PONT' THEN 'PONTIAC'

        WHEN 'SUBA' THEN 'SUBARU'

        WHEN 'VOLV' THEN 'VOLVO'

        WHEN upper(trim(make)) IN (
            'BUIC', 'BUICK', 'BUICK1', 'BUIK'
        ) THEN 'BUICK'

        WHEN upper(trim(make)) IN (
            'CCHEVROLET', 'CHEV', 'CHEVY'
        ) THEN 'CHEVROLET'

        WHEN upper(trim(make)) IN (
            'FOD', 'FOR', 'FORD', 'FRD', 'FROD'
        ) THEN 'FORD'

        WHEN upper(trim(make)) IN (
            'GM', 'GMC', 'GMC1', 'G M C'
        ) THEN 'GMC'

        WHEN upper(trim(make)) IN (
            'INFI', 'INFINITY'
        ) THEN 'INFINITI'

        WHEN upper(trim(make)) IN (
            'LEXU', 'LEXS'
        ) THEN 'LEXUS'

        WHEN upper(trim(make)) IN (
            'MERCEDEZ', 'MERZ'
        ) THEN 'MERCEDES'

        WHEN upper(trim(make)) IN (
            'NISS', 'NISSIAN'
        ) THEN 'NISSAN'

        WHEN upper(trim(make)) IN (
            'TOY', 'TOYO', 'TOYOA', 'TOYOT',
            'TOYT', 'TOYTA', 'TYOTA', '`TOYOTA'
        ) THEN 'TOYOTA'

        WHEN upper(trim(make)) IN (
            'VOLK', 'VOLKS', 'VOLKSWAGON', 'VW'
        ) THEN 'VOLKSWAGEN'

        ELSE 'NOT DETERMINED'
    END AS make_clean
FROM traffic_violations;

-- 3.
-- Normalize traffic stop hours from time stop
SELECT
    time_of_stop,
    to_char(time_of_stop::time, 'FMHH12 am') AS stop_hour_readable
FROM traffic_violations;



