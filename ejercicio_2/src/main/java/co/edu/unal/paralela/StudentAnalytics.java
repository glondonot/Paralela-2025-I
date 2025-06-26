package co.edu.unal.paralela;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Una clase 'envoltorio' (wrapper) para varios métodos analíticos.
 */
public final class StudentAnalytics {
    /**
     * Calcula secuencialmente la edad promedio de todos los estudientes registrados y activos 
     * utilizando ciclos.
     *
     * @param studentArray Datos del estudiante para la clase.
     * @return Edad promedio de los estudiantes registrados
     */
    public double averageAgeOfEnrolledStudentsImperative(
            final Student[] studentArray) {
        List<Student> activeStudents = new ArrayList<Student>();

        for (Student s : studentArray) {
            if (s.checkIsCurrent()) {
                activeStudents.add(s);
            }
        }

        double ageSum = 0.0;
        for (Student s : activeStudents) {
            ageSum += s.getAge();
        }

        return ageSum / (double) activeStudents.size();
    }

    /**
     * PARA HACER calcular la edad promedio de todos los estudiantes registrados y activos usando
     * streams paralelos. Debe reflejar la funcionalidad de 
     * averageAgeOfEnrolledStudentsImperative. Este método NO debe utilizar ciclos.
     *
     * @param studentArray Datos del estudiante para esta clase.
     * @return Edad promedio de los estudiantes registrados
     */
    public double averageAgeOfEnrolledStudentsParallelStream(
            final Student[] studentArray) {
        return Arrays.stream(studentArray) // 1. Convierte el arreglo en un stream 
                    .parallel() // 2. Convierte el stream en paralelo 
                    .filter(s -> s.checkIsCurrent()) // 3. Filtra solo los estudiantes activos 
                    .mapToDouble(s -> s.getAge()) // 4. Mapea cada objeto Student a su edad (un double)
                    .average() // 5. Operación terminal que calcula el promedio 
                    .getAsDouble(); // 6. Extrae el valor del OptionalDouble resultante
    }

    /**
     * Calcula secuencialmente -usando ciclos- el nombre más común de todos los estudiantes 
     * que no están activos en la clase.
     *
     * @param studentArray Datos del estudiante para esta clase.
     * @return Nombre más común de los estudiantes inactivos.
     */
    public String mostCommonFirstNameOfInactiveStudentsImperative(
            final Student[] studentArray) {
        List<Student> inactiveStudents = new ArrayList<Student>();

        for (Student s : studentArray) {
            if (!s.checkIsCurrent()) {
                inactiveStudents.add(s);
            }
        }

        Map<String, Integer> nameCounts = new HashMap<String, Integer>();

        for (Student s : inactiveStudents) {
            if (nameCounts.containsKey(s.getFirstName())) {
                nameCounts.put(s.getFirstName(),
                        new Integer(nameCounts.get(s.getFirstName()) + 1));
            } else {
                nameCounts.put(s.getFirstName(), 1);
            }
        }

        String mostCommon = null;
        int mostCommonCount = -1;
        for (Map.Entry<String, Integer> entry : nameCounts.entrySet()) {
            if (mostCommon == null || entry.getValue() > mostCommonCount) {
                mostCommon = entry.getKey();
                mostCommonCount = entry.getValue();
            }
        }

        return mostCommon;
    }

    /**
     * PARA HACER calcula el nombre más común de todos los estudiantes que no están activos
     * en la clase utilizando streams paralelos. Debe reflejar la funcionalidad 
     * de mostCommonFirstNameOfInactiveStudentsImperative. Este método NO debe usar ciclos
     *
     * @param studentArray Datos de estudiantes para la clase.
     * @return Nombre más comun de los estudiantes inactivos.
     */
    public String mostCommonFirstNameOfInactiveStudentsParallelStream(
            final Student[] studentArray) {
        return Arrays.stream(studentArray) // 1. Convierte el arreglo en un stream
                    .parallel() // 2. Lo hace paralelo
                    .filter(s -> !s.checkIsCurrent()) // 3. Filtra para obtener estudiantes inactivos
                    .collect(Collectors.groupingBy(Student::getFirstName, Collectors.counting())) // 4. Agrupa por nombre y cuenta las ocurrencias
                    .entrySet() // 5. Obtiene un conjunto de entradas del mapa (nombre, conteo)
                    .stream() // 6. Convierte ese conjunto en un nuevo stream
                    .max(Map.Entry.comparingByValue()) // 7. Busca la entrada con el valor (conteo) máximo
                    .get() // 8. Obtiene la entrada del Optional resultante
                    .getKey(); // 9. Extrae la clave (el nombre) de la entrada
    }

    /**
     * calcula secuencialmente el número de estudiantes que han perdido el curso 
     * que son mayores de 20 años. Una calificación de perdido es cualquiera por debajo de 65 
     * 65. Un estudiante ha perdido el curso si tiene una calificación de perdido 
     * y no está activo en la actuialidad
     *
     * @param studentArray Datos del estudiante para la clase.
     * @return Cantidad de calificacione sperdidas de estudiantes mayores de 20 años de edad.
     */
    public int countNumberOfFailedStudentsOlderThan20Imperative(
            final Student[] studentArray) {
        int count = 0;
        for (Student s : studentArray) {
            if (!s.checkIsCurrent() && s.getAge() > 20 && s.getGrade() < 65) {
                count++;
            }
        }
        return count;
    }

    /**
     * PARA HACER calcular el número de estudiantes que han perdido el curso 
     * que son mayores de 20 años de edad . una calificación de perdido está por debajo de 65. 
     * Un estudiante ha perdido el curso si tiene una calificación de perdido 
     * y no está activo en la actuialidad. Debe reflejar la funcionalidad de 
     * countNumberOfFailedStudentsOlderThan20Imperative. El método no debe usar ciclos.
	 *
     * @param studentArray Datos del estudiante para la clase.
     * @return Cantidad de calificacione sperdidas de estudiantes mayores de 20 años de edad.
     */
    public int countNumberOfFailedStudentsOlderThan20ParallelStream(
            final Student[] studentArray) {
        return (int) Arrays.stream(studentArray) // 1. Convierte el arreglo en un stream
                            .parallel() // 2. Lo hace paralelo
                            .filter(s -> !s.checkIsCurrent() && // 3. Filtra usando las tres condiciones
                                        s.getAge() > 20 &&
                                        s.getGrade() < 65)
                            .count(); // 4. Cuenta los elementos que pasaron el filtro 
    }
}
