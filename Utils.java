import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class Utils {

    private static final Log log = LogFactory.getLog(Utils.class);

    public static Map<String, Method> getFieldsWithGetter(Class<?> clazz) {
        HashMap<String, Method> map=new HashMap<>();
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz, Object.class);
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor pd: propertyDescriptors) {
                map.put(pd.getName(), pd.getReadMethod());
            }
        }
        catch (Exception e) {
            log.error(e);
        }
        return map;
    }

}
