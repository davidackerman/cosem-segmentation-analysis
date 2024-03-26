package org.janelia.cosem.util;
import java.io.File;
import java.util.ArrayList;
import java.util.List;


//from chatgpt
public class ListDeepestSubdirectories {
    public static void main(String[] args) {
        File directory = new File("/path/to/your/directory"); // Specify the directory path here
        List<String> deepestSubdirectories = listDeepestSubdirectories(directory);
        System.out.println("Deepest subdirectories:");
        for (String subdirectory : deepestSubdirectories) {
            System.out.println(subdirectory);
        }
    }

    public static List<String> listDeepestSubdirectories(File directory) {
        List<String> deepestSubdirectories = new ArrayList<>();
        if (directory.isDirectory()) {
            File[] subdirectories = directory.listFiles(File::isDirectory);
            if (subdirectories != null && subdirectories.length > 0) {
                for (File subdirectory : subdirectories) {
                    deepestSubdirectories.addAll(listDeepestSubdirectories(subdirectory)); // Recursively list subdirectories
                }
            } else {
                // If no child directories, then it's one of the deepest subdirectories
                deepestSubdirectories.add(directory.getAbsolutePath());
            }
        }
        return deepestSubdirectories;
    }
}