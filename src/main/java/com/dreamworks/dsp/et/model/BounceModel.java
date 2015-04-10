package com.dreamworks.dsp.et.model;

/**
 * Created by mmonti on 4/7/15.
 */
public class BounceModel {

    private Long id;
    private String name;
    private int age;

    public BounceModel() {
    }

    public BounceModel(Long id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "PersonModel{" + "id=" + id + ", name='" + name + '\'' + ", age=" + age + '}';
    }
}
