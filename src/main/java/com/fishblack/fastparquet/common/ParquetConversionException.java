package com.fishblack.fastparquet.common;

public class ParquetConversionException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	private final ErrorCode errorCode;
	
	public static enum ErrorCode {
		INVALID_TYPE_OPTIONS, IO_EXCEPTION, ILLEGAL_ARGUMENT, UNKNOWN, METADATA_FETCH_ERROR, DATA_LENGTH_ERROR
	}

	public ParquetConversionException(String message, ErrorCode errorCode) {
		super(message);
		this.errorCode = errorCode;
	}

	public ParquetConversionException(String message, ErrorCode errorCode, Throwable cause) {
		super(message, cause);
		this.errorCode = errorCode;
	}
	
	public ErrorCode getErrorCode() {
		return errorCode;
	}

}
