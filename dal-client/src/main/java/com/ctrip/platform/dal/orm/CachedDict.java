package com.ctrip.platform.dal.orm;

import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

public class CachedDict {
    Map<Class<?>,Field> keyHolderDict = new HashMap();
    HashSet<Class<?>> shieldDict = new HashSet();
    Map<Class<?>,String> keyNameDict = new HashMap<>();
    Map<Class<?>,Field> shareKeyDict = new HashMap<>();

    void fillDict(Class<?> clazz){
        fillKeyHolderDict(clazz);
        fillKeyName(clazz);
        fillShareKey(clazz);
    }

    private void fillKeyHolderDict(Class<?> clazz) {
        List<Field> tp = Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> {
                    Annotation[] annotations = field.getAnnotations();
                    return Arrays.stream(annotations)
                            .anyMatch(annotation -> annotation instanceof GeneratedValue &&
                                    ((GeneratedValue) annotation).strategy() == GenerationType.AUTO) &&
                            Arrays.stream(annotations).anyMatch(annotation -> annotation instanceof Id) &&
                            Arrays.stream(annotations).anyMatch(annotation -> annotation instanceof Type &&
                                    (((Type) annotation).value() == Types.BIGINT ||
                                            ((Type) annotation).value() == Types.INTEGER ||
                                            ((Type) annotation).value() == Types.SMALLINT));
                }).collect(Collectors.toList());

        if (tp.size() == 1) {
            tp.get(0).setAccessible(true);
            keyHolderDict.put(clazz, tp.get(0));
        }
    }

    private void fillKeyName(Class<?> clazz) {

        List<Field> tp = Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> field.getAnnotationsByType(Id.class).length > 0 &&
                        field.getAnnotationsByType(Column.class).length > 0)
                .collect(Collectors.toList());

        if (tp.size() == 1) {
            Column column = tp.get(0).getAnnotation(Column.class);
            String name = column.name();
            keyNameDict.put(clazz, name);
        }
    }

    private void fillShareKey(Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        Field[] tp = Arrays.stream(fields)
                .filter(field -> (field.getType() == Long.class || field.getType() == Integer.class) && field.isAnnotationPresent(ShareKey.class))
                .toArray(Field[]::new);

        if (tp.length == 1) {
            tp[0].setAccessible(true);
            shareKeyDict.put(clazz, tp[0]);
        }
    }
}
