# pylint: disable=super-init-not-called

class TapSalesforceException(Exception):
    pass

class TapSalesforceQuotaExceededException(TapSalesforceException):
    pass

class TapSalesforceBulkAPIDisabledException(TapSalesforceException):
    pass

class RetriableError(TapSalesforceException):
    pass