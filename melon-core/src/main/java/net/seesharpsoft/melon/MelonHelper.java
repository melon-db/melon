package net.seesharpsoft.melon;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MelonHelper {

    public static final String REFERENCE_MARKER = "$";
    public static final String REFERENCE_ARGUMENT_SEPARATOR = ":";

    public static boolean isReferenceValue(String propertyValue) {
        return propertyValue != null && propertyValue.startsWith(REFERENCE_MARKER) && propertyValue.endsWith(REFERENCE_MARKER);
    }

    public static ReferenceType getReferenceType(String propertyValue) {
        if (!isReferenceValue(propertyValue)) {
            return null;
        }

        int endIndex = propertyValue.indexOf(REFERENCE_ARGUMENT_SEPARATOR);
        if (endIndex == -1) {
            endIndex = propertyValue.length() - 1;
        }
        String referenceName = propertyValue.substring(1, endIndex);
        for (ReferenceType referenceType : ReferenceType.values()) {
            if (referenceName.equalsIgnoreCase(referenceType.name())) {
                return referenceType;
            }
        }
        return ReferenceType.UNKNOWN;
    }

    public static String getReferenceArgument(String propertyValue) {
        if (!isReferenceValue(propertyValue)) {
            return null;
        }

        int startIndex = propertyValue.indexOf(REFERENCE_ARGUMENT_SEPARATOR);
        if (startIndex == -1) {
            return null;
        }
        return propertyValue.substring(startIndex + 1, propertyValue.length() - 1);
    }

    public static File getFile(String fileName) {
        return getFile(fileName, null);
    }

    public static File getFile(String fileName, String reference) {
        String path = fileName;
        if (reference != null && !fileName.startsWith("/") && !fileName.startsWith("\\")) {
            path = reference + "/" + fileName;
        }
        URL url = MelonHelper.class.getResource(path);
        if (url == null) {
            return new File(path);
        }
        return new File(url.getFile());
    }

    public static List<List<String>> deepCopyRecords(List<List<String>> records) {
        return records.stream().map(list -> new ArrayList<>(list)).collect(Collectors.toList());
    }
}
