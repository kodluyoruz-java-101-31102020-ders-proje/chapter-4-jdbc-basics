package db.operations.basic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Main {

	private final static String dbHost = "jdbc:mysql://127.0.0.1:3306/company_db?useSSL=false";
	private final static String userName = "root";
	private final static String password = "root";
	private final static String jdbcDriver = "com.mysql.jdbc.Driver";
	
	public static void main(String[] args) {
	
		
		// employees tablosuna bir tane yeni kayıt ekler.
		// insertEmployeeOneRecord();
				
		// employees tablosundaki tüm kayıtları çeker
		// selectEmployees();
		
		// employees tablosundan 10 nolu kaydı getirir
		// Long empNo = findEmployeeId(10L);
		// System.out.println(empNo);
		
		// employees tablosuna toplu şekilde yeni kayıtları ekler 
		// insertBulkEmployees();
		
		
		// empNo = findEmployeeId(2L);
		// employees tablosunda 6 nolu kaydı günceller
		// updateOneEmployeeRecord(11l, "Batuhan", "Düzgün");
		
		// employees tablosunda toplu şekilde 1,2,3 nolu kayıtları günceller
		// updateBulkEmployeeRecord("Sami", "Hami");
		
		
		// employees tablosunda 3 nolu kaydı siler
		// Long empNoForDeleted = findEmployeeId(2L);
		// deleteOneEmployeeRecord(empNoForDeleted);
	
		
		// employees tablosunda 1,2 nolu kayıtları siler
		// deleteBulkEmployeeRecord();
		
		
		// JDBC API ile transaction yönetimi yapılması
		// transactionManagementInJdbc();
		
		// Savepoint Example
		// savePointExample();
	}
	
	private static void selectEmployees() {
		
		Connection dbConnection = null;
		
		try {
				
			Class.forName(jdbcDriver);
			dbConnection = DriverManager.getConnection(dbHost, userName, password);
			Statement statement = dbConnection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT * FROM employees_auto_inc");
			
			System.out.println("employees_auto_inc all records");
			while(resultSet.next()) {
				
				printEmployeeFormattedText(resultSet);
				
			}
			System.out.println();
			
			// bir önceki satıra geri dönersiniz.
			// resultSet.previous()
			
			resultSet.first();
			System.out.println("First record");
			printEmployeeFormattedText(resultSet);
			System.out.println();
			
			
			resultSet.last();
			System.out.println("Last record");
			printEmployeeFormattedText(resultSet);
			System.out.println();
			
			int rowNumber = 6;
			
			resultSet.absolute(rowNumber);
			
			System.out.println(rowNumber + ". record");
			printEmployeeFormattedText(resultSet);
			System.out.println();
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if(dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	private static Long findEmployeeId(Long empNo) {
		
		Connection dbConnection = null;
		
		try {
			Class.forName(jdbcDriver);
			dbConnection = DriverManager.getConnection(dbHost, userName, password);
			
			PreparedStatement preparedStatement =
					dbConnection.prepareStatement("SELECT emp_no, first_name, last_name FROM employees_auto_inc WHERE emp_no = ? ");
			
			preparedStatement.setLong(1, empNo);
			
			ResultSet resultSet = preparedStatement.executeQuery();
			
			resultSet.first();
			
			Long id = resultSet.getLong(1);
			// Long id = resultSet.getLong("emp_no");
			
			String name = resultSet.getString(2);
			// String name = resultSet.getString("first_name");
			
			String lastName = resultSet.getString(3);
			// String lastName = resultSet.getString("last_name");
			
			System.out.println(id + " " + name + " " + lastName);
			
			return id;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if(dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		return -1L;
	}
	
	private static void insertEmployeeOneRecord() {
		
		Connection dbConnection = null;
		
		try {
			
			Class.forName(jdbcDriver);
			dbConnection = DriverManager.getConnection(dbHost, userName, password);
			
			PreparedStatement preparedStatement = 
					dbConnection.prepareStatement("INSERT INTO employees_auto_inc (emp_no, first_name, last_name, gender, birth_date, hire_date) VALUES(?,?,?,?,?,?)");
			
			preparedStatement.setLong(1, 0);
			preparedStatement.setString(2, "Mehmet");
			preparedStatement.setString(3, "Ali");
			preparedStatement.setString(4, "M");
			preparedStatement.setDate(5, new java.sql.Date(new Date().getTime()));
			preparedStatement.setDate(6, new java.sql.Date(new Date().getTime()));
			
			int insertedRowCount = preparedStatement.executeUpdate();
			
			System.out.println(insertedRowCount + " record inserted!");
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if(dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	private static void insertBulkEmployees() {
		
		Connection dbConnection = null;
		
		Employee employee1 = new Employee();
		employee1.setId(0L);
		employee1.setName("Halil");
		employee1.setLastName("Kılıç");
		employee1.setGender("M");
		employee1.setHireDate(new Date());
		employee1.setBirthDate(new Date());
		
		Employee employee2 = new Employee();
		employee2.setId(0L);
		employee2.setName("Zeynep");
		employee2.setLastName("Altın");
		employee2.setGender("F");
		employee2.setHireDate(new Date());
		employee2.setBirthDate(new Date());
		
		Employee employee3 = new Employee();
		employee3.setId(0L);
		employee3.setName("Can");
		employee3.setLastName("Uzunyol");
		employee3.setGender("M");
		employee3.setHireDate(new Date());
		employee3.setBirthDate(new Date());
		
		List<Employee> employees = new ArrayList<Employee>();
		employees.add(employee1);
		employees.add(employee2);
		employees.add(employee3);
		
		try {
			
			Class.forName(jdbcDriver);
			dbConnection = DriverManager.getConnection(dbHost, userName, password);
			
			PreparedStatement preparedStatement = 
					dbConnection.prepareStatement("INSERT INTO employees_auto_inc (emp_no, first_name, last_name, gender, birth_date, hire_date) VALUES(?,?,?,?,?,?)");

			
			Iterator<Employee> iterator = employees.iterator();
			
			while(iterator.hasNext()) {
				
				Employee employee = iterator.next();
				
				preparedStatement.setLong(1, employee.getId());
				preparedStatement.setString(2, employee.getName());
				preparedStatement.setString(3, employee.getLastName());
				preparedStatement.setString(4, employee.getGender());
				preparedStatement.setDate(5, new java.sql.Date(employee.getBirthDate().getTime()));
				preparedStatement.setDate(6, new java.sql.Date(employee.getHireDate().getTime()));
				
				preparedStatement.addBatch();          
			}
			
			int[] effectedRows = preparedStatement.executeBatch();
			
			System.out.println(effectedRows.length + " rows inserted in employees_auto_inc table!");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if(dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void updateOneEmployeeRecord(Long empNo, String name, String lastName) {
		
		Connection dbConnection = null;
		
		try {
			Class.forName(jdbcDriver);
			dbConnection = DriverManager.getConnection(dbHost, userName, password);
			
			PreparedStatement preparedStatement = 
					dbConnection.prepareStatement("UPDATE employees_auto_inc SET first_name = ?, last_name = ? WHERE emp_no = ? ");
			
			preparedStatement.setString(1, name);
			preparedStatement.setString(2, lastName);
			preparedStatement.setLong(3, empNo);
			
			int updatedRowCount = preparedStatement.executeUpdate();
			System.out.println(updatedRowCount + " rows updated!");
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if(dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void updateBulkEmployeeRecord(String name, String lastName) {
		
		Connection dbConnection = null;
		
		Set<Long> empNoList = new HashSet<Long>();
		empNoList.add(1L);
		empNoList.add(2L);
		empNoList.add(3L);
		
		try {
			Class.forName(jdbcDriver);
			dbConnection = DriverManager.getConnection(dbHost, userName, password);
			
			PreparedStatement preparedStatement = 
					dbConnection.prepareStatement("UPDATE employees_auto_inc SET first_name = ?, last_name = ? WHERE emp_no = ? ");
			
			
			Iterator<Long> iterator = empNoList.iterator();
			
			while(iterator.hasNext()) {
				
				Long empNo = iterator.next();
				preparedStatement.setString(1, name);
				preparedStatement.setString(2, lastName);
				preparedStatement.setLong(3, empNo);
				
				preparedStatement.addBatch();
			}
			
			int[] updatedRowCount = preparedStatement.executeBatch();
			System.out.println(updatedRowCount.length + " rows updated!");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if(dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	private static void deleteOneEmployeeRecord(Long empNo) {
		
		Connection dbConnection = null;
		
		try {
			
			Class.forName(jdbcDriver);
			dbConnection = DriverManager.getConnection(dbHost, userName, password);

			PreparedStatement preparedStatement = 
					dbConnection.prepareStatement("DELETE FROM employees_auto_inc WHERE emp_no = ?");
			
			preparedStatement.setLong(1, empNo);
			
			int deletedRowCount = preparedStatement.executeUpdate();
			System.out.println(deletedRowCount + " rows deleted!");
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if(dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void deleteBulkEmployeeRecord() {
		
		Connection dbConnection = null;
		
		Set<Long> empNoList = new HashSet<Long>();
		empNoList.add(findEmployeeId(1L));
		empNoList.add(findEmployeeId(2L));
		
		try {
			
			Class.forName(jdbcDriver);
			dbConnection = DriverManager.getConnection(dbHost, userName, password);

			PreparedStatement preparedStatement = 
					dbConnection.prepareStatement("DELETE FROM employees_auto_inc WHERE emp_no = ?");
			
			
			Iterator<Long> iterator = empNoList.iterator();
			
			while(iterator.hasNext()) {
				
				Long empNo = iterator.next();
				
				preparedStatement.setLong(1, empNo);
				
				preparedStatement.addBatch();
			}
			
			int[] deletedRowCount = preparedStatement.executeBatch();
			System.out.println(deletedRowCount.length + " rows deleted!");
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if(dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	private static void transactionManagementInJdbc() {
		
		Connection dbConnection = null;
		
		try {
			
			Class.forName(jdbcDriver);
			dbConnection = DriverManager.getConnection(dbHost, userName, password);
			// JDBC normalde otomatik transaction management'ı kendisi yapar.
			// false' a çekerek Transaction management'ı yazılımcı olarak biz üstleniyoruz.
			dbConnection.setAutoCommit(false);
			
			PreparedStatement preparedStatement = 
					dbConnection.prepareStatement("INSERT INTO employees_auto_inc (emp_no, first_name, last_name, gender, birth_date, hire_date) VALUES(?,?,?,?,?,?)");
			
			preparedStatement.setLong(1, 0);
			preparedStatement.setString(2, "Ayşe");
			preparedStatement.setString(3, "Kalem");
			preparedStatement.setString(4, "F");
			preparedStatement.setDate(5, new java.sql.Date(new Date().getTime()));
			preparedStatement.setDate(6, new java.sql.Date(new Date().getTime()));
			
			int insertedRowCount = preparedStatement.executeUpdate();
			
			System.out.println(insertedRowCount + " record inserted!");
			
			/* Sembolik olarak hata oluşturuyoruz. Hata oluşunca kayıt veritabanına yansımıyor.
			 * Çünkü, Transaction'da hata oluşursa rollback ediyoruz.
			if(insertedRowCount == 1) {
				throw new RuntimeException("Waowww SQL Exception!");
			}
			*/
			
			if(insertedRowCount == 1) {
				throw new RuntimeException("Waowww SQL Exception!");
			}
			
			dbConnection.commit();
			
		}
		catch (Exception e) {
			
			e.printStackTrace();
			
			try {
				// Hata olursa rollback edip tüm değişiklikleri geri alıyoruz.
				dbConnection.rollback();
				
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		finally {
			if(dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	private static void savePointExample() {
		
		Connection dbConnection = null;
		Savepoint savepointForModification = null;
		
		try {
			
			Class.forName(jdbcDriver);
			dbConnection = DriverManager.getConnection(dbHost, userName, password);
			dbConnection.setAutoCommit(false);
			
			String generatedColumns[] = { "emp_no" };
			
			// operation - 1 insert record!
			PreparedStatement preparedStatement = 
					dbConnection.prepareStatement("INSERT INTO employees_auto_inc (emp_no, first_name, last_name, gender, birth_date, hire_date) VALUES(?,?,?,?,?,?)", 
											generatedColumns);
			
			preparedStatement.setLong(1, 0);
			preparedStatement.setString(2, "SAVEPOINT1");
			preparedStatement.setString(3, "SAVEPOINT1");
			preparedStatement.setString(4, "F");
			preparedStatement.setDate(5, new java.sql.Date(new Date().getTime()));
			preparedStatement.setDate(6, new java.sql.Date(new Date().getTime()));
			
			int insertedRowCount = preparedStatement.executeUpdate();
			System.out.println(insertedRowCount + " rows inserted!");
			
			ResultSet rs = preparedStatement.getGeneratedKeys();
			long empNo = 0;
			if (rs.next()) {
				empNo = rs.getLong(1);
			    System.out.println("Inserted ID - " + empNo);
			}
			
			// operation - 2 update record!
			preparedStatement = 
					dbConnection.prepareStatement("UPDATE employees_auto_inc SET first_name = ?, last_name = ? WHERE emp_no = ? ");
			
			preparedStatement.setString(1, "Mustafa");
			preparedStatement.setString(2, "Ertaç");
			preparedStatement.setLong(3, empNo);
			
			int updatedRowCount = preparedStatement.executeUpdate();
			System.out.println(updatedRowCount + " rows updated!");
			
			
			// create save point!
			savepointForModification = dbConnection.setSavepoint("RECORD_MODIFICATION");
			
			
			// operation - 3 delete record!
			preparedStatement = 
					dbConnection.prepareStatement("DELETE FROM employees_auto_inc WHERE emp_no = ?");
			
			preparedStatement.setLong(1, empNo);
			
			int deletedRowCount = preparedStatement.executeUpdate();
			if(deletedRowCount == 1)
			{
				throw new RuntimeException("Waowww SQL Exception!");
			}
			
			System.out.println(deletedRowCount + " rows deleted!");
			
			dbConnection.commit();
		}
		catch (Exception e) {
			
			e.printStackTrace();
			
			try {
				
				if(savepointForModification == null) {
					dbConnection.rollback();
				}
				else {
					dbConnection.rollback(savepointForModification);
					dbConnection.commit();
				}
				
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		finally {
			if(dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void printEmployeeFormattedText(ResultSet resultSet) throws SQLException {
		
		StringBuilder builder = new StringBuilder();
		builder.append("Record - " + resultSet.getRow());
		builder.append(" ====> ");
		builder.append(resultSet.getLong("emp_no"));
		builder.append(" ");
		builder.append(resultSet.getString("first_name"));
		builder.append(" ");
		builder.append(resultSet.getString("last_name"));
		builder.append(" ");
		builder.append(resultSet.getString("gender"));
		builder.append(" ");
		builder.append(resultSet.getDate("birth_date"));
		builder.append(" ");
		builder.append(resultSet.getDate("hire_date"));
		builder.append(" ");
		
		System.out.println(builder.toString());
	}

}
