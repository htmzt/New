
def get_financial_aggregations():
    """
    Financial metrics that we always want to calculate.
    Returns SQL fragment for SELECT clause.
    """
    return """
        COUNT(*) as total_records,
        COUNT(DISTINCT po_id) as unique_pos,
        COALESCE(SUM(line_amount), 0) as total_line_amount,
        COALESCE(SUM(ac_amount), 0) as total_ac_amount,
        COALESCE(SUM(pac_amount), 0) as total_pac_amount,
        COALESCE(SUM(remaining), 0) as total_remaining_amount
    """


def get_status_aggregations():
    """
    Status breakdown counts.
    Returns SQL fragment for SELECT clause.
    """
    return """
        COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) as closed_count,
        COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) as cancelled_count,
        COUNT(CASE WHEN status LIKE '%Pending%' THEN 1 END) as pending_count
    """


def get_payment_terms_aggregations():
    """
    Payment terms breakdown.
    Returns SQL fragment for SELECT clause.
    """
    return """
        COUNT(CASE WHEN payment_terms = 'ACPAC 100%' THEN 1 END) as acpac_100_count,
        COUNT(CASE WHEN payment_terms = 'AC1 80 | PAC 20' THEN 1 END) as ac_pac_split_count
    """


def get_category_aggregations():
    """
    Category breakdown by type.
    Returns SQL fragment for SELECT clause.
    """
    return """
        COUNT(CASE WHEN category = 'Survey' THEN 1 END) as survey_count,
        COUNT(CASE WHEN category = 'Transportation' THEN 1 END) as transportation_count,
        COUNT(CASE WHEN category = 'Site Engineer' THEN 1 END) as site_engineer_count,
        COUNT(CASE WHEN category = 'Service' THEN 1 END) as service_count
    """


def get_date_range_aggregations():
    """
    Date range for the period.
    Returns SQL fragment for SELECT clause.
    """
    return """
        MIN(publish_date) as earliest_date,
        MAX(publish_date) as latest_date
    """


def get_period_grouping(period_type: str):
    """
    Get SQL for date grouping based on period type.
    
    This determines HOW we group the data by time.
    
    Args:
        period_type: "weekly", "monthly", or "yearly"
    
    Returns:
        SQL fragment for SELECT clause with period-specific columns
    """
    if period_type == "weekly":
        # PostgreSQL week logic:
        # - DATE_TRUNC('week', date) gives the Monday of that week
        # - EXTRACT(WEEK FROM date) gives week number (1-53)
        # - EXTRACT(YEAR FROM ...) ensures we group by year too
        return """
            DATE_TRUNC('week', publish_date) as period_date,
            TO_CHAR(DATE_TRUNC('week', publish_date), 'YYYY-MM-DD') as period_label,
            EXTRACT(YEAR FROM DATE_TRUNC('week', publish_date)) as year,
            EXTRACT(WEEK FROM publish_date) as week_number
        """
    
    elif period_type == "monthly":
        # Extract year and month separately for grouping
        return """
            EXTRACT(YEAR FROM publish_date) as year,
            EXTRACT(MONTH FROM publish_date) as month,
            TO_CHAR(publish_date, 'Month YYYY') as period_label
        """
    
    elif period_type == "yearly":
        # Just group by year
        return """
            EXTRACT(YEAR FROM publish_date) as year,
            CAST(EXTRACT(YEAR FROM publish_date) AS TEXT) as period_label
        """
    
    else:
        raise ValueError(f"Unknown period type: {period_type}")


def get_period_filter(period_type: str, year=None, month=None, week=None):
    """
    Get SQL WHERE clause filter for specific period.
    
    This lets users filter to a specific time range.
    
    Args:
        period_type: "weekly", "monthly", or "yearly"
        year: Optional year filter
        month: Optional month filter (only for monthly)
        week: Optional week filter (only for weekly)
    
    Returns:
        Tuple of (where_clause, params_dict)
    """
    conditions = []
    params = {}
    
    # Year filter (works for all period types)
    if year:
        conditions.append("EXTRACT(YEAR FROM publish_date) = :year")
        params["year"] = year
    
    # Month filter (only for monthly summaries)
    if month and period_type == "monthly":
        conditions.append("EXTRACT(MONTH FROM publish_date) = :month")
        params["month"] = month
    
    # Week filter (only for weekly summaries)
    if week and period_type == "weekly":
        conditions.append("EXTRACT(WEEK FROM publish_date) = :week")
        params["week"] = week
    
    # If no conditions, return "1=1" (always true, no filtering)
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    return where_clause, params