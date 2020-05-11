package com.fishblack.fastparquet.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConvertResult {
    private static final Logger logger = Logger.getLogger(ConvertResult.class.getName());

    public static final String AUDIT_CONVERT_RESULT_KEY="CONVERT_RESULT";
    public static final String AUDIT_DETAIL_MESSAGE_KEY="DETAIL_MESSAGE";

    /**
     * Constructor
     */
    public ConvertResult(){}

    public enum Result {
        SUCCESS,
        PARTIAL_SUCCESS,
        FAILED
    }

    private Result result;
    private long totalCount;
    private long failureCount;
    private long successCount;
    private Map<Long, Map<String, String>> errors = new ConcurrentHashMap<>();

    @JsonProperty("result")
    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    @JsonProperty("total_count")
    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    @JsonProperty("failure_count")
    public long getFailureCount() {
        return failureCount;
    }

    /**
     * Set this failure count will trigger constraint validation
     * @param failureCount
     */
    public void setFailureCount(long failureCount) {
        this.failureCount = failureCount;
    }

    @JsonProperty("success_count")
    public long getSuccessCount() {
        return successCount;
    }

    public void setSuccessCount(long successCount) {
        this.successCount = successCount;
    }

    @JsonProperty("errors")
    public Map<Long, Map<String, String>> getErrors() {
        return errors;
    }

    public void setErrors(Map<Long, Map<String, String>> errors) {
        this.errors = errors;
    }

    /**
     * Generate Json string of this result.
     * @return Json string of this result.
     */
    public String toJSON() {
        try {
            return (new ObjectMapper()).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.log(Level.WARNING, "Cannot convert ConvertResult to JSON", e);
            return "[Cannot convert ConvertResult to JSON]";
        }
    }

    private double getRuntimeErrorPercentage(){
        double percentage = 0;
        long currentCount = this.failureCount + this.successCount;
        if (currentCount > 0){
            percentage = (double) this.failureCount * 100 / currentCount;
        }
        return percentage;
    }

    /**
     * Get runtime statistic of conversion
     * @return InstantStatistic
     */
    @JsonIgnore
    public InstantStatistic getSnapshot(){
        return new InstantStatistic(this.getFailureCount(), this.getRuntimeErrorPercentage());
    }

    /**
     * Statistic for runtime conversion result at this moment
     */
    public class InstantStatistic {
        private long failureCount;
        private double failurePercentage;

        public InstantStatistic(long failureCount, double failurePercentage){
            this.failureCount = failureCount;
            this.failurePercentage = failurePercentage;
        }

        public long getFailureCount() {
            return failureCount;
        }

        public double getFailurePercentage() {
            return failurePercentage;
        }

    }
}
