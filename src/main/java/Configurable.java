/**
 * Created by aminerounak on 3/10/14.
 */
public interface Configurable {
    public Long getDataLong(String k);
    public Integer getDataInt(String k);
    public Boolean getDataBool(String k);
    public String getDataStr(String k);
    public boolean putDataStr(String k, String v);
    public boolean putDataBool(String k, Boolean v);
    public boolean putDataLong(String k, Long v);
    public boolean putDataInt(String k, Integer v);
}
