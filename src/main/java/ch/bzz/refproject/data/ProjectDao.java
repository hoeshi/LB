package ch.bzz.refproject.data;

import ch.bzz.refproject.model.Category;
import ch.bzz.refproject.model.Project;
import ch.bzz.refproject.util.Result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class ProjectDao implements Dao<Project, String>{
    /**
     * get als projects
     * @return projects
     */
    @Override
    public List<Project> getAll(){
        ResultSet resultSet;
        List<Project> projectList = new ArrayList<>();
        String sqlQuery =
                "SELECT p.pprojectUUID, c.categoryUUID, p.title," +
                        " p.startdate, p.enddate, p.status" +
                        " FROM Project AS p JOIN Project AS c USING (categoryUUID)" +
                        " WHERE status = A";

        try {
            resultSet = MySqlDB.sqlSelect(sqlQuery);
            while (resultSet.next()) {
                Project project = new Project();
                setValues(resultSet, project);
                projectList.add(project);
            }
        } catch (SQLException sqlEx) {

            sqlEx.printStackTrace();
            throw new RuntimeException();
        } finally {

            MySqlDB.sqlClose();
        }
        return projectList;

    }

    @Override
    public Project getEntity(String projectUUID) {
        Connection connection;
        PreparedStatement prepStmt;
        ResultSet resultSet;
        Project project = new Project();

        String sqlQuery =
                "SELECT p.projectUUID, p.title, p.title, p.startdate, p.enddate, " +
                        "  p.status" +
                        "  FROM Project AS p JOIN Project AS c USING (categoryUUID)" +
                        " WHERE projectUUID='" + projectUUID.toString() + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            resultSet = prepStmt.executeQuery();
            if (resultSet.next()) {
                setValues(resultSet, project);
            }

        } catch (SQLException sqlEx) {

            sqlEx.printStackTrace();
            throw new RuntimeException();
        } finally {
            MySqlDB.sqlClose();
        }
        return project;

    }

    @Override
    public Result delete(String projectUUID) {
        Connection connection;
        PreparedStatement prepStmt;
        String sqlQuery =
                "DELETE FROM Project" +
                        " WHERE projectUUID='" + projectUUID.toString() + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            int affectedRows = prepStmt.executeUpdate();
            if (affectedRows == 1) {
                return Result.SUCCESS;
            } else if (affectedRows == 0) {
                return Result.NOACTION;
            } else {
                return Result.ERROR;
            }
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw new RuntimeException();
        }

    }

    @Override
    public Result save(Project project) {
        Connection connection;
        PreparedStatement prepStmt;
        String sqlQuery =
                "REPLACE Book" +
                        " SET projectUUID='" + project.getProjectUUID() + "'," +
                        " title='" + project.getTitle() + "'," +
                        " startdate='" + project.getStartDate() + "'," +
                        " enddate=" + project.getEndDate() + "," +
                        " categoryUUID='" + project.getCategory().getCategoryUUID() + "'," +
                        " status='" + project.getStatus() + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            int affectedRows = prepStmt.executeUpdate();
            if (affectedRows <= 2) {
                return Result.SUCCESS;
            } else if (affectedRows == 0) {
                return Result.NOACTION;
            } else {
                return Result.ERROR;
            }
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw new RuntimeException();
        }

    }

    private void setValues(ResultSet resultSet, Project project) throws SQLException {
        project.setProjectUUID(resultSet.getString("projectUUID"));
        project.setTitle(resultSet.getString("title"));
        project.setStartDate(resultSet.getString("startdate"));
        project.setEndDate(resultSet.getString("enddate"));
        project.setStatus(resultSet.getString("status"));
        project.setCategory(new Category());
        project.getCategory().setCategoryUUID(resultSet.getString("categoryUUID"));

    }

}
